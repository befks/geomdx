GeoMDX
======

Implementation of a "GEO" OLAP driver for the usage in GeoServer.
Mondrian has been used as OLAP server but any server supported by olap4j should work.
The GeoDefinitions are defined via WKT definitions to links in the cube.

INSTALL
=======
Geoserver gets the definitions out of a tabel with the following format and defined in OLAP or the database

CREATE TABLE querycatalog
(
  name character varying(32) NOT NULL,
  mdx text NOT NULL,
  geometrytype character varying(32) NOT NULL,
  geometrycolumn character varying(32) NOT NULL,
  columntype character varying(255),
  CONSTRAINT querycatalog_pkey PRIMARY KEY (name)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE querycatalog

This tabel is used by the geoserver to get the defined queries

Query Definitions
=================
insert into querycatalog(name, mdx, geometrytype, geometrycolumn, columntype)
values('Naam van de laag','MDX Query', 'MultiPolygon', 'Source_Location', 'zone');

•   Name            Name that will be seen in the GeoServer Layer tabe.
•   MDX Query       The MDX query which will be used. The actual implementation is working with 3 types of geometries SuperZone, zone and subzone
•   GeometryColumn  Name of the geoMetrycolum
•   ColumnType      superzone, zone of subzone

GeoServer adding jars
=====================

The following jars have to be coied into the lib directory of GeoServer
•   gt-mdx-1.0.jar
•   olap4j.jar
•   olap4j-xmla.jar
•   xercesImpl.jar

Store Definition
================
A new store can now be defined with the following remarks:

•   server      URL van de mondrian xmla server
•   refresh     Number of hours the query is cached
•   srid        srid van de geografische data.
•   GeometryProcessor       be.fks.feathers.geoserver.FeathersWKTCache
•   GeometryProcessorConfig Config parameter voor de processor

The geometry call is now the class that will be used to make geografies out of the "OLAP" field
•   COPY dim_geography TO '/tmp/dump.txt' WITH DELIMITER '#';

License
=======




More information
================
For More Information http://www.fks.be/


