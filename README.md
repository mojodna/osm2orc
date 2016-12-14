# OSM2ORC

Transcodes [OSM PBF files](https://wiki.openstreetmap.org/wiki/PBF_Format) to [ORC](http://orc.apache.org/).

## Schema

The following schema should also work with Presto, Spark, and Hive (the location protocol may need to be changed to `s3a:` and `hadoop-aws-2.7.3.jar` and `aws-java-sdk-1.7.4.jar` (*not* the latest) must be available in your `CLASSPATH`).

**IF YOU'RE NOT RUNNING IN THE AWS `us-east-1` REGION, PLEASE COPY `s3://osm.mojodna.net/planet-2016-11-30/planet-2016-11-30.orc` INTO YOUR OWN BUCKET / HDFS**

```sql
CREATE EXTERNAL TABLE planet (
    id BIGINT,
    type STRING,
    tags MAP<STRING,STRING>,
    lat DECIMAL(9,7),
    lon DECIMAL(10,7),
    nds ARRAY<STRUCT<ref: BIGINT>>,
    members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
    changeset BIGINT,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT
)
STORED AS ORCFILE
LOCATION 's3://osm.mojodna.net/planet-2016-11-30/';
```

```sql
CREATE EXTERNAL TABLE planet_history (
    id BIGINT,
    type STRING,
    tags MAP<STRING,STRING>,
    lat DECIMAL(9,7),
    lon DECIMAL(10,7),
    nds ARRAY<STRUCT<ref: BIGINT>>,
    members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
    changeset BIGINT,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT,
    visible BOOLEAN
)
STORED AS ORCFILE
LOCATION 's3://osm.mojodna.net/planet/';
```

**NOTE**: `osm.mojodna.net` is in AWS's `us-east-1` region, so **please** make sure that you're using Athena in the same region, for both performance and cost reasons.

## Sample Queries

### Re-assemble Ways

```sql
WITH nodes AS (
  SELECT
    id,
    tags,
    lat,
    lon
  FROM planet
  WHERE type = 'node'
),
ways AS (
  SELECT
    id,
    tags,
    nds
  FROM planet
  WHERE type = 'way'
),
nodes_in_bbox AS (
  SELECT *
  FROM nodes
  WHERE lon BETWEEN -121.4024 AND -121.2483
    AND lat BETWEEN 43.9992 AND 44.1250
),
-- fetch and expand referenced ways
referenced_ways AS (
  SELECT
    ways.*,
    t.*
  FROM ways
  CROSS JOIN UNNEST(nds) WITH ORDINALITY AS t (nd, idx)
  JOIN nodes_in_bbox nodes ON nodes.id = nd.ref
),
-- fetch *all* referenced nodes (even those outside the bbox)
exploded_ways AS (
  SELECT
    ways.id,
    ways.tags,
    idx,
    nd.ref,
    nodes.id node_id,
    ARRAY[nodes.lat, nodes.lon] coordinates
  FROM referenced_ways ways
  JOIN nodes ON nodes.id = nd.ref
  ORDER BY ways.id, idx
),
ways_in_bbox AS (
  SELECT
    id,
    arbitrary(tags) tags,
    array_agg(coordinates) coordinates
  FROM exploded_ways
  GROUP BY id
)
-- relations are more difficult; ways must be reassembled recursively
SELECT
  id,
  'node' type,
  tags,
  ARRAY[ARRAY[lat, lon]] coordinates
FROM nodes_in_bbox
UNION ALL
SELECT
  id,
  'way' type,
  tags,
  coordinates
FROM ways_in_bbox
```

Get information about the most recent version of all non-deleted entities:

```sql
SELECT planet.*
FROM planet_history planet
INNER JOIN (
  SELECT id,
         type,
         MAX(version) version
  FROM planet_history
  GROUP BY type, id
) latest
  ON planet.id = latest.id
    AND planet.version = latest.version
    AND planet.type = latest.type
WHERE planet.visible = true
ORDER BY
  CASE planet.type
    WHEN 'node' THEN 1
    WHEN 'way' THEN 2
    WHEN 'relation' THEN 3
    ELSE 4
  END,
  planet.id
```

Get the number of deleted entities (this will probably time out):

```sql
WITH latest AS (
  SELECT planet.*
  FROM planet_history planet
  INNER JOIN (
    SELECT id,
           type,
           MAX(version) version
    FROM planet_history
    GROUP BY type, id
  ) latest
    ON planet.id = latest.id
      AND planet.version = latest.version
      AND planet.type = latest.type
  WHERE planet.visible = false
  ORDER BY
    CASE planet.type
      WHEN 'node' THEN 1
      WHEN 'way' THEN 2
      WHEN 'relation' THEN 3
      ELSE 4
    END,
    planet.id
)
SELECT count(*) FROM latest
```

## Osmosis Plugin

```bash
gradle jar
cp builds/libs/osm2orc-1.0-SNAPSHOT.jar $OSMOSIS_HOME/lib/default

osmosis --rbf ~/src/mojodna/osm2orc/delaware-latest.osm.pbf --write-orc delaware.orc
osmosis --read-xml-change 694.osc.gz --convert-change-to-full-history --write-orc 694.osc.orc
```

The following dependencies (available from a `distZip`) also need to be copied to `$OSMOSIS_HOME/lib/default` (TODO create a plugin jar that only contains osm2orc + these):

* `aircompressor-0.3.jar`
* `commons-collections-3.2.2.jar`
* `commons-configuration-1.6.jar`
* `commons-lang-2.6.jar`
* `guava-16.0.1.jar`
* `hadoop-auth-2.6.4.jar`
* `hadoop-common-2.6.4.jar`
* `hive-storage-api-2.1.1.2-pre-orc.jar`
* `orc-core-1.2.2.jar)`

`OSMOSIS_HOME` when installed via [Homebrew](https://brew.sh) is `/usr/local/opt/osmosis/libexec`.
