# OSM2ORC

Transcodes [OSM PBF files](https://wiki.openstreetmap.org/wiki/PBF_Format) to [ORC](http://orc.apache.org/).

Sample outputs are also available via S3 for querying with AWS Athena.

## Schema

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
LOCATION 's3://osm-pds/planet/';
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
LOCATION 's3://osm-pds/planet-history/';
```

```sql
CREATE EXTERNAL TABLE changesets (
    id BIGINT,
    tags MAP<STRING,STRING>,
    created_at TIMESTAMP,
    open BOOLEAN,
    closed_at TIMESTAMP,
    comments_count BIGINT,
    min_lat DECIMAL(9,7),
    max_lat DECIMAL(9,7),
    min_lon DECIMAL(10,7),
    max_lon DECIMAL(10,7),
    num_changes BIGINT,
    uid BIGINT,
    user STRING
)
STORED AS ORCFILE
LOCATION 's3://osm-pds/changesets/';
```

**NOTE**: `osm-pds` is in AWS's `us-east-1` region, so please make sure that you're using Athena in the same region, for both performance and cost reasons.

The schemas should also work with Presto, Spark, and Hive, although the location protocol may need to be changed to `s3a:` and `hadoop-aws-2.7.3.jar` and `aws-java-sdk-1.7.4.jar` (*not* the latest) must be available in your `CLASSPATH`.

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

Get usages of `tracktype=*` by month:

```sql
SELECT date_trunc('month', timestamp) month, count(*) count
FROM planet_history
WHERE tags['tracktype'] IS NOT NULL
GROUP BY date_trunc('month', timestamp)
ORDER BY date_trunc('month', timestamp)
```

(This would be better implemented using a window function to only count the
_additions_ of a tag, rather than edits to an entity that uses it.)

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

Count how many changesets have a comment tag (h/t [ToeBee/ChangesetMD](https://github.com/ToeBee/ChangesetMD)):

```sql
SELECT count(*)
FROM changesets
WHERE tags['comment'] IS NOT NULL
```

Find all changesets that were created by JOSM:

```sql
SELECT count(*)
FROM changesets
WHERE tags['created_by'] LIKE 'JOSM%'
```

Find all changesets that were created in Liberty Island:

```sql
SELECT count(id)
FROM changesets
WHERE min_lon BETWEEN -74.0474545 AND -74.0433990
  AND max_lon BETWEEN -74.0474545 AND -74.0433990
  AND min_lat BETWEEN 40.6884971 AND 40.6911817
  AND max_lat BETWEEN 40.6884971 AND 40.6911817
```

## Build

```bash
./gradlew distTar
```

Now, cd into the distribution directory and untar:

```bash
cd build/distributions
tar xzvf osm2orc-1.0-SNAPSHOT.tar
cd osm2orc-1.0-SNAPSHOT/bin
```

Your executable is in the `bin/` directory of your distribution.

## Run

To convert an OSM PBF to ORC:

```
./osm2orc <osm-pbf-input> <osm-orc-output>
```

To convert an OSM Changeset XML to ORC:

``` 
./osm2orc --changeset <osm-changeset-xml-input> <osm-changeset-orc-output>
```

## Develop

You can easily develop and debug on this project in IntelliJ IDEA.

1. In IDEA's startup menu, select `Import Project`.
2. `Import project from external model`. Select `Gradle`.
3. Select:
    * Use auto-import
    * Create directories for empty content roots automatically
    * Create separate module per source set
    * Use default gradle wrapper (recommended)

Leave `Gradle home` blank. The defaults are fine. Finish.

Once you have your project loaded, all you have to do is run / debug
`Osm2Orc.main()`. You can supply args in the `Run/Debug Configurations`.
The tool needs at least an input and output path as described above.

## Osmosis Plugin

```bash
gradle jar
cp builds/libs/osm2orc-1.0-SNAPSHOT.jar $OSMOSIS_HOME/lib/default

osmosis --rbf delaware-latest.osm.pbf --write-orc delaware.orc
osmosis --rb history-161205.osm.pbf --write-orc planet.osh.orc
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
* `hive-storage-api-2.2.0.jar`
* `log4j-1.2.17.jar`
* `orc-core-1.3.0.jar`
* `slf4j-api-1.7.10.jar`
* `slf4j-log4j12-1.7.5.jar`

`OSMOSIS_HOME` when installed via [Homebrew](https://brew.sh) is `/usr/local/opt/osmosis/libexec`.
