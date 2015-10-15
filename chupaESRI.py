#!/usr/bin/env python
# chupaESRI.py
# version: 0.1 (2014-01-21)
# author: John Reiser <jreiser@njgeo.org>
#
# ChupaESRI provides you with functions to make importing features returned from a 
# ArcGIS Server Map Service Query request into a PostgreSQL database.
# When run from the command line with the sufficient number of arguments, the script
# will extract all point or polygon features from a service and import them into a new 
# PostGIS table.
#
# Feel free to contact the author with questions or comments.
# Any feedback or info on how this is being used is greatly appreciated.
#
# Copyright (C) 2014, John Reiser
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
import traceback

class EsriJSON2Pg(object):
    """Convert ESRI JSON response from ArcGIS Server's Query service to PostgreSQL CREATE TABLE and INSERTs."""
    def __init__(self, jsonstr):
        if(type(jsonstr) == type("") and len(jsonstr) > 0):
            cleanjson = re.sub(r'\\r\\n', '\\r\\n', jsonstr)
            self.srcjson = json.loads(cleanjson)
            esriTypes = {
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

    def checkOIDrange(self, webconn, url):
        ## TODO: how to handle an "objectid" field that's not called "objectid"?
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
            print "Base ArcGIS Server url: /{0}/rest/services/".format(dpr.group(1))
            webconn.request('GET', '/{0}/rest/services/?f=pjson'.format(dpr.group(1)))
        else:
            webconn.request('GET', "/arcgis/rest/services/?f=pjson")
        webresp = webconn.getresponse()
        version = json.loads(webresp.read())['currentVersion']
        if(version >= 10.1):
            qs = """?where=&outFields=*&returnGeometry=false&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=[{%0D%0A++++"statisticType"%3A+"count"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidcount"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidmin"%0D%0A++}%2C{%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A++++"onStatisticField"%3A+"objectid"%2C+++++"outStatisticFieldName"%3A+"oidmax"%0D%0A++}]&returnZ=false&returnM=false&returnDistinctValues=false&f=pjson"""
        else:
            qs = """?text=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&objectIds=&where=objectid+>+-1&time=&returnCountOnly=true&returnIdsOnly=false&returnGeometry=false&maxAllowableOffset=&outSR=&outFields=&f=pjson"""
        try:
            webconn.request('GET', url+qs)
            webresp = webconn.getresponse()
            response = json.loads(webresp.read())
            if(version >= 10.1):
                oid = response['features'][0]['attributes']
            else: 
                oid = {'oidmin':0, 'oidmax':response['count']}  ## look into why certain instances return this in all caps versus all lower
            self.oidrange = oid
            return [(f, f+999) for f in xrange(oid['oidmin'], oid['oidmax'], 1000)]
            #### TO-DO: probably should have it look for maxRecordCount to populate the range
        except Exception as e:
            print "Encountered an error:"
            print e
            print url+qs
            print response

    def convertFields(self):
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
                    if f.has_key('type'):
                        field['type'] = ft[ f['type'] ]
                        if field['type'] in ('date', 'uuid', 'integer', 'bigint', 'smallint'):
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
                if len(ring) < 3:
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
    
    def insertStatements(self, tablename="{tablename}", upsert=False):
        i = 0
        while i < len(self.srcjson['features']):
#            data = self.srcjson['features'][i]['attributes']
            data = {}
            for k,v in self.srcjson['features'][i]['attributes'].iteritems():
                data[ self.cleanFieldNames(k) ] = v
            data['shape'] = self.changeGeometry(indx=i)
            if not data['shape'] == None:
                if upsert:
                    pass
                else:
                    sql = "INSERT INTO {0} ({1}) VALUES ({2});".format(tablename, ",".join(map(lambda x: x['name'], self.fields)), ",".join(map(lambda x: "%("+x['name']+")s", self.fields)))
                yield (sql, data)
            i += 1
        

if __name__ == "__main__":
    import sys
    import psycopg2
    # run on the command line:
    # argv[1]: ArcGIS Server REST API, query endpoint
    # example: "http://example.com:6080/rest/services/Base/MapServer/0/query"
    # argv[2]: PostgreSQL connection string
    # example: "host=localhost dbname=gisdata user=gisadmin password=P4ssW0rd"
    # argv[3]: Schema-qualified table name
    # example: "gisdata.tablename"
    
    if len(sys.argv) < 4:
        print "Too few parameters.\nUSAGE: {0} restapiurl pgconnstr tblname".format(sys.argv[0])
        sys.exit(2)

    urlm = re.match("https?://([\w\:\.\-]+)(/.*)", sys.argv[1])
    domain = urlm.groups()[0]
    path   = urlm.groups()[1]
    print domain
    if(sys.argv[1][:5].lower() == 'https'):
        webconn = httplib.HTTPSConnection(domain, timeout=360)
    else:
        webconn = httplib.HTTPConnection(domain, timeout=360)
    oids = EsriJSON2Pg("").checkOIDrange(webconn, path)

    ct = True # flag for creating a table
    conn = psycopg2.connect(sys.argv[2])
    cur = conn.cursor()
    
    for l in oids:
        print "Requesting {0} <= objectid <= {1}".format(l[0],l[1])
        try:
            qs = "?where=objectid+>%3D+{0}+AND+objectid+<%3D+{1}&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&f=pjson".format(l[0],l[1])
            webconn.request('GET', path+qs)
            webresp = webconn.getresponse()
            arcjson = webresp.read()
            jp = EsriJSON2Pg(arcjson)
            if ct:
                cur.execute(jp.createTable(sys.argv[3]))
                ct = False
            i = 0
            for data in jp.insertStatements(tablename=sys.argv[3]):
                if not data[1] == None:
                    cur.execute(data[0], data[1])
                i += 1
            conn.commit()
        except Exception, e:
            print e
            print traceback.format_exc()
            print "Failed on: ",path+qs
            break
