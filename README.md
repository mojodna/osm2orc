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

**NOTE**: `osm.mojodna.net` is in AWS's `us-east-1` region, so **please** make sure that you're using Athena in the same region, for both performance and cost reasons.

Re-assemble ways

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
