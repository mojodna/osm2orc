CREATE EXTERNAL TABLE seattle_nodes (
    changeset BIGINT,
    id BIGINT,
    tags MAP<STRING,STRING>,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT,
    visible BOOLEAN,
    lat DECIMAL(9,7),
    lon DECIMAL(10,7)
)
STORED AS ORCFILE
LOCATION 's3://mojodna-temp/seattle_nodes/';

CREATE EXTERNAL TABLE seattle_ways (
    changeset BIGINT,
    id BIGINT,
    tags MAP<STRING,STRING>,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT,
    visible BOOLEAN,
    nds ARRAY<STRUCT<ref: BIGINT>>
)
STORED AS ORCFILE
LOCATION 's3://mojodna-temp/seattle_ways/';

CREATE EXTERNAL TABLE seattle_relations (
    changeset BIGINT,
    id BIGINT,
    tags MAP<STRING,STRING>,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT,
    visible BOOLEAN,
    members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>
)
STORED AS ORCFILE
LOCATION 's3://mojodna-temp/seattle_relations/';

MSCK REPAIR TABLE seattle_nodes

SELECT ways.id, idx, nd.ref, nodes.lat, nodes.lon, nodes.tags, ways.tags
FROM seattle_ways ways 
CROSS JOIN UNNEST(nds) WITH ORDINALITY AS t (nd, idx)
JOIN seattle_nodes nodes ON nodes.id=nd.ref
WHERE ways.tags['leisure'] = 'park'
ORDER BY ways.id, idx ASC
