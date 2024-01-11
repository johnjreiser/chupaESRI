#!/usr/bin/env python3
"""
chupaESRI.py
version:           0.4 (2019-07-22)
author:            John Reiser <jreiser@njgeo.org>
addl contributors: Connor Hornibrook <cfbrooks94@gmail.com>

ChupaESRI provides you with functions to make importing features returned from a
ArcGIS Server Map Service Query request into a PostgreSQL database.
When run from the command line with the sufficient number of arguments, the script
will extract all point or polygon features from a service and import them into a new
PostGIS table.

Changes
0.4
    - upgraded to Python 3
    - support for -srid flag to optionally configure the output spatial reference system
    - some other refactoring

0.3 - checks for the existence of the table and if objectids have already been downloaded

0.2 - revised escaped character handling; added "fudge factor" to character varying type

Feel free to contact the author with questions or comments.
Any feedback or info on how this is being used is greatly appreciated.

Copyright (C) 2014 John Reiser
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import re
import os
import psycopg2
import argparse
import logging
import requests
from urllib.parse import quote

_web_mercator = 3857
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
        level=os.environ.get('LOGLEVEL', 'WARNING').upper()
    )

objectid_field_name = "OBJECTID"

class QueryException(Exception):
    """
    Basic custom exception
    """
    def __init__(self, msg):
        """
        Constructor
        :param msg: The message to send the user
        """
        super().__init__(msg)


class EsriJSON2Pg(object):
    """
    Convert ESRI JSON response from ArcGIS Server's Query service to PostgreSQL CREATE TABLE and INSERTs.
    """

    # esri geometry type to postgis geometry type
    _esri_types = {
        "esriGeometryNull": "GEOMETRY",
        "esriGeometryPoint": "POINT",
        "esriGeometryMultipoint": "MULTIPOINT",
        "esriGeometryLine": "LINESTRING",
        "esriGeometryCircularArc": "CURVE",
        "esriGeometryEllipticArc": "CURVE",
        "esriGeometryBezier3Curve": "CURVE",
        "esriGeometryPath": "CURVE",
        "esriGeometryPolyline": "MULTILINESTRING",
        "esriGeometryRing": "POLYGON",
        "esriGeometryPolygon": "MULTIPOLYGON",
        "esriGeometryEnvelope": "POLYGON",
        "esriGeometryAny": "GEOMCOLLECTION",
        "esriGeometryMultiPatch": "MULTISURFACE",
        "esriGeometryTriangleStrip": "MULTISURFACE",
        "esriGeometryTriangleFan": "MULTISURFACE",
        "esriGeometryTriangles": "MULTISURFACE"
    }

    # from http://edndoc.esri.com/arcobjects/9.2/ComponentHelp/esriGeodatabase/esriFieldType.htm
    # esri non-geom type to Postgresql non-geom type
    _field_translation = {
        "esriFieldTypeSmallInteger": "integer",
        "esriFieldTypeInteger": "integer",
        "esriFieldTypeSingle": "double precision",
        "esriFieldTypeDouble": "double precision",
        "esriFieldTypeString": "character varying",
        "esriFieldTypeDate": "bigint",
        "esriFieldTypeOID": "serial",
        "esriFieldTypeBlob": "bytea",
        "esriFieldTypeGUID": "uuid",
        "esriFieldTypeGlobalID": "uuid",
        "esriFieldTypeXML": "xml"
    }

    def __init__(self, in_json, output_table, out_srid=None):
        """
        Constructor
        :param in_json:      The json from a GET request for data from an ArcGIS REST endpoint
        :param output_table: The name of the destination table
        :param out_srid:     The output spatial reference system id, if the user desires a different one
                             from that of the source data
        """
        if in_json:
            self.srcjson = in_json
            self.out_table = output_table
            if "geometryType" in self.srcjson:
                self.geomType = self._esri_types[self.srcjson['geometryType']]
            else:
                self.geomType = None

            if out_srid:
                self.sr = out_srid
            else:
                if "spatialReference" in self.srcjson:
                    if "latestWkid" in self.srcjson["spatialReference"]:
                        self.sr = self.srcjson["spatialReference"]['latestWkid']
                    elif "wkid" in self.srcjson["spatialReference"]:
                        self.sr = self.srcjson["spatialReference"]['wkid']
                    else:
                        self.sr = -1
                    if self.sr == 102100:
                        self.sr = _web_mercator
                        # done to fix the ESRI number for Web Mercator
                else:
                    self.sr = -1
            self.fields = self.convert_fields()
        else:
            raise ValueError

    def convert_fields(self):
        """
        Translate esri fields to normal table fields
        :return: Converted fields
        """
        fudge = 5  # default value to pad out character varying types
        fo = []
        for f in self.srcjson['fields']:
            field = {}
            if "name" in f:
                has_type_key = "type" in f
                field['name'] = _clean_field_names(f['name'])
                # remove duplicate names
                if not field['name'] in map(lambda x: x['name'], fo):
                    for item in ("length", "alias"):
                        if item in f:
                            field[item] = f[item]
                            if item == 'length' and has_type_key and f['type'] == "esriFieldTypeString":
                                field[item] = int(field[item]) + fudge  # padding to help with escaped characters
                    if has_type_key:
                        field['type'] = self._field_translation[f['type']]
                        if field['type'] in ('date', 'uuid', 'integer', 'bigint', 'smallint'):
                            field.pop('length', None)
                    if field['name']:
                        fo.append(field)
        if self.geomType:
            fo.append({'name': 'shape', 'type': self.geomType})
        return fo

    def create_table(self):
        """
        :return: sql for creating the needed destination table
        """
        sql = f"CREATE TABLE {self.out_table} ("
        fields = []
        for f in self.fields:
            if "." not in f['name']:
                if f['name'] not in ("geometry", "shape"):
                    fd = f"\n{f['name']} \t{f['type']}"
                    if "length" in f:
                        fd = fd + f" ({f['length']})"
                    fields.append(fd)
        sql = sql + ",".join(fields)
        sql = sql + "\n);\n"
        if "geometryType" in self.srcjson:
            if "spatialReference" in self.srcjson:
                if "." in self.out_table:
                    sql += f"SELECT AddGeometryColumn('{self.out_table.split('.')[0]}', " \
                           f"'{self.out_table.split('.')[1]}', 'shape', {self.sr}, '{self.geomType}', 2, True);"
                else:
                    sql += f"SELECT AddGeometryColumn('{self.out_table}', 'shape', " \
                           f"{self.sr}, '{self.geomType}', 2, True);"
        for f in self.fields:
            if "alias" in f:
                if not f['name'] == f['alias']:
                    sql += f"\nCOMMENT ON COLUMN {self.out_table}." + f['name'] + " IS '" + f['alias'] + "';"
        return sql

    def change_geometry(self, geom=None, indx=None):
        """
        Method that creates wkt strings for geometries
        :param geom: The input geometry
        :param indx: The index of the incoming data
        :return:     wkt string
        """
        if geom == indx is None:
            return None
        if indx > -1:
            geom = self.srcjson['features'][indx]['geometry']
        if 'rings' in geom:
            if not geom['rings']:
                return None
            else:
                for ring in geom['rings']:
                    if len(ring) <= 3:  # needed to trim slivers/self-intersections
                        return None

            if self.geomType in ("POLYGON", "MULTIPOLYGON"):
                wkt = f"SRID={self.sr};{self.geomType}" + str(geom['rings']).replace("[","(").replace("]",")")
                wkt = re.sub(r'(\d)\,', r'\1', wkt)
                return wkt.replace("), (", ",").replace(u"\x01", "")
            if self.geomType in ("LINESTRING", "MULTILINESTRING"):
                pass
                # todo: write this.
        else:
            if self.geomType == "POINT":
                wkt = f"SRID={self.sr};{self.geomType}({geom['x']} {geom['y']})"
                return wkt
        return None

    def insert_statements(self, upsert=False):
        """
        Parses insert statements using the source json data
        :param upsert: Whether or not this is an upsert
        :return:
        """
        i = 0
        while i < len(self.srcjson['features']):
            data = {}
            for k, v in self.srcjson['features'][i]['attributes'].items():
                data[_clean_field_names(k)] = v
            data['shape'] = self.change_geometry(indx=i)
            if data['shape']:
                sql = ""
                if not upsert:
                    sql = f"INSERT INTO {self.out_table} ({','.join(map(lambda x: x['name'], self.fields))}) " \
                          f"VALUES ({','.join(map(lambda x: '%('+x['name']+')s', self.fields))});"
                yield dict(sql=sql, data=data)
            i += 1


def _valid_table(in_tbl):
    """
    Checks that given table name is a valid Postgresql table name (schema-qualified)
    :param in_tbl:
    :return:
    """
    if re.match(re.compile(r"^\w+\.\w+$"), in_tbl):
        return in_tbl
    else:
        raise IOError("Invalid schema-qualified table name entered.")


def _valid_endpoint(in_url):
    """
    Checks that a given url is valid
    :param in_url: The input url
    :return:       The url, if valid
    """
    in_url = in_url.strip()
    check_url = in_url[:-5] if in_url[-5:] == "query" else in_url
    if 200 <= requests.get(check_url).status_code < 300:
        return in_url
    else: 
        raise IOError("Invalid endpoint entered.")


def _clean_field_names(name):
    """
    Cleans the incoming field name
    :param name: The field name
    :return:     The field name without any parentheses
    """
    if "." in name:
        name = name.split(".")[-1:][0].replace("(", "").replace(")", "")
    return name


def _get_endpoint_destination(in_endpoint_url):
    """
    Splits up the parts of an input REST url
    :param in_endpoint_url: The input url
    :return:                A dict containing the domain name and the endpoint name
    """
    try:
        groups = re.match(r"https?://([\w\:\.\-]+)(/.*)", in_endpoint_url).groups()
        domain = f"{'https' if in_endpoint_url[:5] == 'https' else 'http'}://{groups[0]}"
        return dict(domain=domain, path=groups[1])
    except IndexError:
        raise IOError("Invalid url entered.")

def _validate_srid(in_srid):
    """
    Validation for SRID arg parsing
    :param in_srid: The input spatial reference id
    :return:        The srid, if valid
    """
    msg = "Invalid SRID, must be an int or string."
    if isinstance(in_srid, int) or not in_srid:
        return in_srid
    elif isinstance(in_srid, str):
        try:
            return int(in_srid)
        except ValueError:
            raise IOError(msg)
    else:
        raise IOError(msg)


def _check_oid_range(in_domain, in_path):
    """
    Creates a range of object ids to query on
    :param in_domain: The base domain for the REST url
    :param in_path:   The REST endpoint
    :return:          A range of object ids
    """
    # TODO: how to handle an "objectid" field that's not called "objectid"?
    """[{
        "statisticType": "count",
        "onStatisticField": "objectid",
        "outStatisticFieldName": "oidcount"
      },{
        "statisticType": "min",
        "onStatisticField": "objectid",
        "outStatisticFieldName": "oidmin"
      },{
        "statisticType": "max",
        "onStatisticField": "objectid",
        "outStatisticFieldName": "oidmax"
      }]
    """

    # Handle non-standard ArcGIS Server paths
    dpr = re.match(r"/([\w\/]*)/rest/services", in_path, re.IGNORECASE)
    version_path_url = f"{dpr.group(1) if dpr else 'arcgis'}/rest/services/"
    version = requests.get(f"{in_domain}/{version_path_url}", params=dict(f="pjson")).json()['currentVersion']
    logging.debug(version)
    q_url = f"{in_domain}{in_path}"
    logging.debug(q_url)

    try:
        global objectid_field_name
        oid_query = "?f=json&where=1%3D1&resultRecordCount=1"
        oid_url = q_url + oid_query
        oid_response = requests.get(oid_url).json()
        logging.debug(oid_response)
        if "objectIdFieldName" in oid_response:
            objectid_field_name = oid_response["objectIdFieldName"]
    except Exception as e:
        logging.error(e)

    if version < 10.1:
        q_url += f"?text=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&objectIds=&where={objectid_field_name}+>+-1&time=&returnCountOnly=true&returnIdsOnly=false&returnGeometry=false&maxAllowableOffset=&outSR=&outFields=&f=pjson"
    else:
        q_url += '?where=&outFields=*&returnGeometry=false&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=[{%0D%0A++++"statisticType"%3A+"count"%2C%0D%0A++++"onStatisticField"%3A+"'+objectid_field_name+'"%2C+++++"outStatisticFieldName"%3A+"oidcount"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField"%3A+"'+objectid_field_name+'"%2C+++++"outStatisticFieldName"%3A+"oidmin"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A++++"onStatisticField"%3A+"'+objectid_field_name+'"%2C+++++"outStatisticFieldName"%3A+"oidmax"%0D%0A++}]&returnZ=false&returnM=false&returnDistinctValues=false&f=pjson'
    try:
        response = requests.get(q_url).json()
        if version >= 10.1:
            # force keys to lowercase - not always returned lower
            logging.debug(response)
            oid = dict((k.lower(), v) for k, v in response['features'][0]['attributes'].items())
        else:
            oid = {'oidmin':0, 'oidmax':response['count']}
        return [(f, f + 999) for f in range(oid['oidmin'], oid['oidmax'], 1000)]
        # todo: probably should have it look for maxRecordCount to populate the range
    except Exception as e:
        logging.debug(q_url)
        logging.error(e)
        raise


def _validate_connection_str(in_str):
    """
    Returns a psycopg2.Connection object if the connection string is valid
    :param in_str: The input connection string
    :return:       A connection object, if string is valid
    """
    try:
        return psycopg2.connect(in_str)
    except psycopg2.DatabaseError:
        raise IOError("Invalid connection string.")


def _with_command_line(f):
    """
    Decorator that handles command line arg parsing. Passes in parsed args to decorated function
    :param f: The function to be decorated
    :return:  The decorated function
    """
    def wrap(*args, **kwargs):
        ap = argparse.ArgumentParser()
        ap.add_argument("endpoint", type=_valid_endpoint, help="The ArcGIS Server REST endpoint")
        ap.add_argument("connection", type=_validate_connection_str, help="PostgreSQL connection string. example: \"host=localhost "
                                                                          "dbname=gisdata user=gisadmin password=P4ssW0rd\"")
        ap.add_argument("table", type=_valid_table, help="Schema-qualified table name. Example: \"gisdata.tablename\"")
        ap.add_argument("-srid", "--output-srid", default=None, type=_validate_srid)
        return f(ap.parse_args(), *args, **kwargs)
    return wrap 


@_with_command_line
def main(cmd_line):
    """
    Wrapper function for the main method
    :param cmd_line: The parsed command line args
    :return:
    """
    destination = _get_endpoint_destination(cmd_line.endpoint)
    domain = destination["domain"]
    path = destination["path"]
    tbl_parts = cmd_line.table.split(".")
    oids = _check_oid_range(domain, path)
    cur = cmd_line.connection.cursor()

    # highest record in database table
    db_max = -1

    # check to see if table exists
    table_sql = "select 1 from pg_tables where schemaname = %s and tablename = %s"
    logging.debug( cur.mogrify(table_sql, tbl_parts) )
    cur.execute(table_sql, tbl_parts)

    # record found, table already exists
    if cur.rowcount:
        logging.info("Table exists.")
        table_exists = True
        maxsql = f"select max(objectid) from {cmd_line.table}"
        cur.execute(maxsql)
        row = next(cur)
        db_max = row[0]
        logging.info(f"Highest OID in table: {db_max}")

    # no record returned, table does not exist
    else:
        table_exists = False
        logging.warning(f"{'.'.join(tbl_parts)} does not exist. It will be created.")

    # populate the destination table
    for l in oids:
        if db_max >= l[1]:
            continue
        logging.info(f"Requesting {l[0]} <= objectid <= {l[1]}")
        if table_exists:
            try:
                chksql = f"select 1 from {cmd_line.table} where objectid between %s and %s"
                cur.execute(chksql, l)
                if cur.rowcount > 0:
                    logging.info(f"Record exist; skipping {l[0]} through {l[1]}")
                    continue

            except Exception as e:
                logging.error(e)

        # make request for new records
        try:
            qs = f"?where={objectid_field_name}+>%3D+{l[0]}+AND+{objectid_field_name}+<%3D+{l[1]}&text=&objectIds=&time=&geometry=" \
                 f"&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=" \
                 f"&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false" \
                 f"&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false" \
                 f"&returnM=false&gdbVersion=&returnDistinctValues=false&f=pjson"

            jp = EsriJSON2Pg(requests.get(f"{domain}/{path}{qs}").json(), cmd_line.table,
                             out_srid=cmd_line.output_srid)

            if not table_exists:
                cur.execute(jp.create_table())
            i = 0
            for insert_statement_info in jp.insert_statements():
                if "data" in insert_statement_info:
                    cur.execute(insert_statement_info["sql"], insert_statement_info["data"])
                i += 1
            cmd_line.connection.commit()
        except Exception as e:
            raise QueryException(f"{str(e)}\nFailed on: {path + qs}")

    # cleanup of db objects
    cur.close()
    cmd_line.connection.close()
    del cur


if __name__ == "__main__":
    main()
