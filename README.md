# chupaESRI

## About
ChupaESRI is a Python module/command line tool to extract features from ArcGIS Server map services. 

### Name?
Think "[chupacabra](http://en.wikipedia.org/wiki/Chupacabra)" or "[Chupa Chups](http://en.wikipedia.org/wiki/Chupa_Chups)".

## Dependencies
- Knowledge of the [ArcGIS Server REST API](http://resources.arcgis.com/en/help/arcgis-rest-api/index.html)
- How to set up and configure [PostgreSQL](http://www.postgresql.org/) and [PostGIS](http://postgis.net/)

#### Setup
```bash
pip3 install -r requirements.txt
```

## Usage
When run from the command line, the tool will extract all features from a [Map Service Query](http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Query_Map_Service_Layer/02r3000000p1000000/) endpoint. You must also specify a PostgreSQL connection string and the name of the table where the extracted data will be stored. 

When accessed as a module, the EsriJSON2Pg class is available to you to incorporate into your own projects. See the main method for examples on its use. 
  

## How to Use
Have you encountered an ArcGIS Server instance that contains data that you would like to use, but are limited by the functionality provided by the Map Service?

By browsing the REST API structure, you can locate the Query endpoint for the Layer you desire downloading. For example, the Query endpoint looks similar to the following:
```http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5/query```

    
For example, to download the State polygons from the ArcGIS Server above into a "public.states" table in a local PostgreSQL instance:
```bash
./chupaESRI.py http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5/query "host=localhost dbname=gisdata user=gisadmin password=P4ssW0rd" "public.states"
```

Optionally, you can provide an output SRID for your table to use. If not specified, the program looks for the spatial reference
information using the endpoint.
```bash
./chupaESRI.py http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5/query "host=localhost dbname=gisdata user=gisadmin password=P4ssW0rd" "public.states" -srid 4326
```

## To-Do
- Implement ability to export LINE features
- More intelligent querying of server-provided statistics to better plan the download requests
- More elegant way of identifying non-standard ArcGIS REST URLs
- Development of an ArcGIS Toolbox to perform the extraction, transforming, loading from within ArcGIS

## Disclaimer
Provided as-is. Use at your own risk. Licensed under the GPL v3. Check with the target ArcGIS Server administrator to make sure he/she allows extraction of the data using this tool. 