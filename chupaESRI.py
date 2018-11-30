#!/usr/bin/env python
# chupaESRI.py
# version: 0.4 (2018-11)
# author: John Reiser <jreiser@njgeo.org>
#
# ChupaESRI provides you with functions to make importing features returned from a
# ArcGIS Server Map Service Query request into a PostgreSQL database.
# When run from the command line with the sufficient number of arguments, the script
# will extract all point or polygon features from a service and import them into a new
# PostGIS table.
#
# Changes
# 0.4 - argparse, logging, and additional options
# 0.3 - checks for the existence of the table and if objectids have already been downloaded
# 0.2 - revised escaped character handling; added "fudge factor" to character varying type
#
# Feel free to contact the author with questions or comments.
# Any feedback or info on how this is being used is greatly appreciated.
#
# Copyright (C) 2014 John Reiser
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import re, httplib
import simplejson as json
from urlparse import urlsplit
import string
import traceback
import logging

#logging.basicConfig(filename='chupa.log', level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

class EsriJSON2Pg(object):
    """Convert ESRI JSON response from ArcGIS Server's Query service to PostgreSQL CREATE TABLE and INSERTs."""
    def __init__(self, jsonstr, arcgisurl=None, chunkSize=1000):
        self.chunkSize = int(chunkSize)
        if(type(arcgisurl) == type("")):
            self.remote_url = arcgisurl
            self.remote_url_components = urlsplit(arcgisurl)

        if(type(jsonstr) == type("") and len(jsonstr) > 0):
            self.srcjson = json.loads( jsonstr, strict=False )
            esriTypes = {
                "esriGeometryNull": None,
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
            if self.srcjson.has_key("geometryType"):
                self.geomType = esriTypes[ self.srcjson['geometryType'] ]
            else:
                self.geomType = None

            if(self.srcjson.has_key("spatialReference")):
                if(self.srcjson["spatialReference"].has_key('latestWkid')):
                    self.sr = self.srcjson["spatialReference"]['latestWkid']
                elif(self.srcjson["spatialReference"].has_key('wkid')):
                    self.sr = self.srcjson["spatialReference"]['wkid']
                else:
                    self.sr = -1
                if self.sr == 102100:
                    self.sr = 3857
                    # done to fix the ESRI number for Web Mercator
            else:
                self.sr = -1
            self.fields = self.convertFields()
            self.sql_createtable = self.createTable()
            self.oidrange = {}

    def checkDateField(self, webconn, url=None):
        if url == None:
            url = "".join(self.remote_url_components[2:3])
        pass

    def checkOIDrange(self, webconn, url=None):
        ## TODO: how to handle an "objectid" field that's not called "objectid"?
        if url == None:
            url = "".join(self.remote_url_components[2:3])

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

        ## Handle non-standard ArcGIS Server paths
        pathroot = "/arcgis/rest/services/"
        dpr = re.match(r"/([\w\/]*)/rest/services", url, re.IGNORECASE)
        if(dpr):
            logging.info("Base ArcGIS Server url: /{0}/rest/services/".format(dpr.group(1)) )
            webconn.request('GET', '/{0}/rest/services/?f=pjson'.format(dpr.group(1)))
        else:
            webconn.request('GET', "/arcgis/rest/services/?f=pjson")
        webresp = webconn.getresponse().read()
        logging.debug(webresp)

        # Filter out non-printable characters. 
        # May need to update this for Unicode support. 
        webresp = filter(lambda x: x in string.printable, webresp)

        version = json.loads(webresp)['currentVersion']
        logging.debug("ArcGIS Version: {0}".format(version) )
        if(version >= 10.1):
            qs = """?where=&outFields=*&returnGeometry=false&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=[{%0D%0A++++"statisticType"%3A+"count"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidcount"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidmin"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidmax"%0D%0A++}]&returnZ=false&returnM=false&returnDistinctValues=false&f=pjson"""
            logging.debug('Version greater than or equal to 10.1')
            logging.debug(qs)
        else:
            qs = """?text=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&objectIds=&where=objectid+>+-1&time=&returnCountOnly=true&returnIdsOnly=false&returnGeometry=false&maxAllowableOffset=&outSR=&outFields=&f=pjson"""
            logging.debug('Version less than 10.1')
            logging.debug(qs)
        try:
            webconn.request('GET', url+qs)
            webresp = webconn.getresponse()

            # Filter out non-printable characters. 
            # May need to update this for Unicode support. 
            webresp = filter(lambda x: x in string.printable, webresp.read())

            response = json.loads(webresp)
            logging.debug('>>> OID calculation response >>>')
            logging.debug(response)
            if(version >= 10.1):
                # force keys to lowercase - not always returned lower
                oid = dict((k.lower(),v) for k,v in response['features'][0]['attributes'].iteritems()) 
            else:
                oid = {'oidmin':0, 'oidmax':response['count']}
            self.oidrange = oid
            try:
                return [(f, f+(self.chunkSize-1)) for f in xrange(oid['oidmin'], oid['oidmax'], self.chunkSize)]
            except Exception as e:
                logging.critical("Unable to generate OID ranges.")
                logging.critical(e)
                logging.critical(oid)
                logging.critical(self.chunkSize)
                sys.exit(2)

            #### TO-DO: probably should have it look for maxRecordCount to populate the range
        except Exception as e:
            logging.debug(url+qs)
            logging.debug(response)
            logging.critical(e)
            sys.exit(2)

    def convertFields(self):
        fudge = 5 # default value to pad out character varying types
        ft = { ## from http://edndoc.esri.com/arcobjects/9.2/ComponentHelp/esriGeodatabase/esriFieldType.htm
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
        fo = []
        for f in self.srcjson['fields']:
            field = {}
            if f.has_key('name'):
                field['name'] = self.cleanFieldNames(f['name'])
                # remove duplicate names
                if not field['name'] in map(lambda x: x['name'], fo):
                    for item in ("length", "alias"):
                        if f.has_key(item):
                            field[item] = f[item]
                            if item == 'length' and f.has_key('type') and f['type'] == "esriFieldTypeString":
                                field[item] = int(field[item]) + fudge # padding to help with escaped characters
                    if f.has_key('type'):
                        if field.has_key('length') and field['length'] >= 256 and f['type'] == "esriFieldTypeString":
                            field['type'] = 'text'
                        else:
                            field['type'] = ft[ f['type'] ]
                        if field['type'] in ('date', 'uuid', 'integer', 'bigint', 'smallint', 'text'):
                            field.pop('length', None)
                    if not field['name'] == None:
                        fo.append(field)
        if self.geomType:
            fo.append({'name': 'shape', 'type': self.geomType})
        return fo

    def cleanFieldNames(self, name):
        if "." in name:
            name = name.split(".")[-1:][0].replace("(", "").replace(")", "")
        return name

    def cleanValues(self, value):
        if isinstance(value, str):
            value = filter(lambda x: x in string.printable, value)
        elif isinstance(value, unicode):
            pass
        else:
            return value
        value = " ".join(value.split())
        return value

    def createTable(self, tablename="{tablename}"):
        sql = 'CREATE TABLE '+tablename+' ('
        fields = []
        for f in self.fields:
            if "." not in f['name']:
                if f['name'] not in ("geometry", "shape"):
                    fd = "\n{0} \t{1}".format(f['name'], f['type'])
                    if f.has_key('length'):
                        fd = fd + " ({0})".format(f['length'])
                    fields.append(fd)
        sql = sql + ",".join(fields)
        sql = sql + "\n);\n"
        if self.srcjson.has_key("geometryType"):
            if(self.srcjson.has_key("spatialReference")):
                if "." in tablename:
                    sql = sql + "SELECT AddGeometryColumn('{0}', '{1}', 'shape', {2}, '{3}', 2, True);".format(tablename.split('.')[0], tablename.split('.')[1], self.sr, self.geomType)
                else:
                    sql = sql + "SELECT AddGeometryColumn('{0}', 'shape', {1}, '{2}', 2, True);".format(tablename, self.sr, self.geomType)
        for f in self.fields:
            if f.has_key('alias'):
                if not f['name'] == f['alias']:
                    sql = sql + "\nCOMMENT ON COLUMN {0}.".format(tablename) + f['name'] + " IS '" + f['alias'] + "';"
        logging.debug( sql )
        return sql

    def changeGeometry(self, geom=None, indx=None):
        if(geom == indx == None):
            return None
        if(indx > -1):
            geom = self.srcjson['features'][indx]['geometry']
        if('rings' in geom):
            if(len(geom['rings']) == 0):
                return None
            for ring in geom['rings']:
                if len(ring) <= 3: # needed to trim slivers/self-intersections
                    return None
            if self.geomType in ("POLYGON","MULTIPOLYGON"):
                WKT = "SRID={0};{1}".format(self.sr, self.geomType) + str(geom['rings']).replace("[","(").replace("]",")")
                WKT = re.sub(r'(\d)\,', r'\1', WKT)
                return WKT.replace("), (", ",").replace(u"\x01", "")
            if self.geomType in ("LINESTRING", "MULTILINESTRING"):
                pass
                ## TO-DO: write this.
        else:
            if self.geomType in ("POINT"):
                WKT = "SRID={0};{1}({2} {3})".format(self.sr, self.geomType, geom['x'], geom['y'])
                return WKT
        return None

    def insertStatements(self, tablename="{tablename}", upsert=False, clean=True):
        i = 0
        while i < len(self.srcjson['features']):
#            data = self.srcjson['features'][i]['attributes']
            data = {}
            for k,v in self.srcjson['features'][i]['attributes'].iteritems():
                if clean:
                    data[ self.cleanFieldNames(k) ] = self.cleanValues(v)
                else:
                    data[ self.cleanFieldNames(k) ] = v
            try:
                data['shape'] = self.changeGeometry(indx=i)
            except:
                logging.error( 'Cannot determine shape. Feature index: {0}'.format(i) )
                logging.debug( data )
                sys.exit(2)

            if upsert:
                pass
            else:
                sql = "INSERT INTO {0} ({1}) VALUES ({2});".format(tablename, ",".join(map(lambda x: x['name'], self.fields)), ",".join(map(lambda x: "%("+x['name']+")s", self.fields)))
            
            yield (sql, data)
            i += 1

if __name__ == "__main__":
    import sys
    import psycopg2
    import argparse
    # run on the command line:
    # argv[1]: ArcGIS Server REST API, query endpoint
    # example: "http://example.com:6080/rest/services/Base/MapServer/0/query"
    # argv[2]: PostgreSQL connection string
    # example: "host=localhost dbname=gisdata user=gisadmin password=P4ssW0rd"
    # argv[3]: Schema-qualified table name
    # example: "gisdata.tablename"

    parser = argparse.ArgumentParser(description='chupaESRI: download features from ArcGIS Server REST MapServer endpoints.') 
    parser.add_argument('rest', help='URL to the ArcGIS Server REST Query Endpoint')
    parser.add_argument('pgconn', help='PostgreSQL connection string')
    parser.add_argument('table', help='Schema-qualified table name (e.g. public.housingpts)')
    parser.add_argument('-o', '--oids', nargs=2, type=int, help='Request a specific set of OBJECTIDs, low and high bounds')
    parser.add_argument('-n', '--chunk', type=int, default=1000, help='Size of each request. Default 1000.')

    argv = parser.parse_args()

    chupa = EsriJSON2Pg("", argv.rest, chunkSize=argv.chunk)
    urlm = chupa.remote_url_components
    domain = urlm[1]
    path   = "".join(urlm[2:3])
    logging.debug( domain )
    if(urlm[0].lower() == 'https'):
        webconn = httplib.HTTPSConnection(domain, timeout=360)
    else:
        webconn = httplib.HTTPConnection(domain, timeout=360)
    oids = chupa.checkOIDrange(webconn)

    conn = psycopg2.connect(argv.pgconn)
    cur = conn.cursor()

    dbmax = -1 # highest record in database table
    ct = True # flag for creating a table
    tblsql = "select 1 from pg_tables where schemaname = %s and tablename = %s"
    cur.execute(tblsql,argv.table.split('.'))
    if cur.rowcount > 0:
        logging.info( "Table exists." )
        ct = False
        maxsql = "select max(objectid) from {0}".format(argv.table)
        cur.execute(maxsql)
        row = cur.next()
        dbmax = row[0]
        logging.info( "Highest OID in table: {0}".format(dbmax) )
    else:
        logging.info( "Table does not exist." )

    for l in oids:
        if dbmax >= l[1]:
            continue
        logging.info( "Requesting {0} <= objectid <= {1}".format(l[0],l[1]) )
        if not ct:
            try:
                chksql = "select 1 from {0} where objectid between %s and %s".format(argv.table)
                cur.execute(chksql, l)
                if cur.rowcount > 0:
                    logging.info( "Record exist; skipping {0} through {1}".format(*l) )
                    continue
            except Exception, e:
                logging.error( e )
                logging.error( traceback.format_exc() )
        
        try:
            qs = "?where=objectid+>%3D+{0}+AND+objectid+<%3D+{1}&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&f=pjson".format(l[0],l[1])
            webconn.request('GET', path+qs)
            webresp = webconn.getresponse()

            # Filter out non-printable characters. 
            # May need to update this for Unicode support. 
            # arcjson = filter(lambda x: x in string.printable, webresp.read())
            arcjson = webresp.read()

            logging.debug( arcjson )
            jp = EsriJSON2Pg(arcjson)
            if ct:
                cur.execute(jp.createTable(argv.table))
                ct = False
            i = 0
            for data in jp.insertStatements(tablename=argv.table):
                logging.debug( data )
                if not data[1] == None:
                    try:
                        cur.execute(data[0], data[1])
                    except Exception as e:
                        logging.critical("Unable to execute SQL.")
                        logging.critical(e.message)
                        logging.critical(data)
                        sys.exit(2)
                i += 1
            conn.commit()
        except Exception as e:
            logging.critical(e.message)
            logging.critical(traceback.format_exc())
            logging.critical("Failed on: "+str(path+qs))
            sys.exit(2)
