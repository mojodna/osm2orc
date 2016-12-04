package net.mojodna.osmorc;

import de.topobyte.osm4j.core.access.OsmIterator;
import de.topobyte.osm4j.core.model.iface.*;
import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Map;

import static org.apache.orc.TypeDescription.*;

public class Transcode {
    public static void main(String[] args) throws Exception {
        // TODO create a single schema and write everything to that
        // if split, it can be treated as "partitioned on type"
        TypeDescription nodeSchema = createStruct()
                .addField("changeset", createLong())
                .addField("id", createLong())
                .addField("tags", createMap(
                        createString(),
                        createString()
                ))
                .addField("timestamp", createTimestamp())
                .addField("uid", createLong())
                .addField("user", createString())
                .addField("version", createLong())
                .addField("visible", createBoolean())
                .addField("lat", createDecimal().withScale(7).withPrecision(9))
                .addField("lon", createDecimal().withScale(7).withPrecision(10));

        TypeDescription waySchema = createStruct()
                .addField("changeset", createLong())
                .addField("id", createLong())
                .addField("tags", createMap(
                        createString(),
                        createString()
                ))
                .addField("timestamp", createTimestamp())
                .addField("uid", createLong())
                .addField("user", createString())
                .addField("version", createLong())
                .addField("visible", createBoolean())
                .addField("nds", createList(
                    createStruct()
                        .addField("ref", createLong())
                ));

        TypeDescription relationSchema = createStruct()
                .addField("changeset", createLong())
                .addField("id", createLong())
                .addField("tags", createMap(
                        createString(),
                        createString()
                ))
                .addField("timestamp", createTimestamp())
                .addField("uid", createLong())
                .addField("user", createString())
                .addField("version", createLong())
                .addField("visible", createBoolean())
                .addField("members", createList(
                    createStruct()
                        .addField("type", createString())
                        .addField("ref", createLong())
                        .addField("role", createString())
                ));

        Configuration conf = new Configuration();
        // id can be omitted if elements are assumed to be sorted
        // user can be omitted if separate user mappings are kept (to produce a list of uids from a list of usernames)
//        conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags,type");
        Writer nodeWriter = OrcFile.createWriter(new Path("nodes.orc"),
                OrcFile.writerOptions(conf).setSchema(nodeSchema));
        Writer wayWriter = OrcFile.createWriter(new Path("ways.orc"),
                OrcFile.writerOptions(conf).setSchema(waySchema));
        Writer relationWriter = OrcFile.createWriter(new Path("relations.orc"),
                OrcFile.writerOptions(conf).setSchema(relationSchema));

        InputStream input = new FileInputStream(args[0]);

        OsmIterator iterator = new PbfIterator(input, true);

        VectorizedRowBatch nodeBatch = nodeSchema.createRowBatch();
        VectorizedRowBatch wayBatch = waySchema.createRowBatch();
        VectorizedRowBatch relationBatch = relationSchema.createRowBatch();

        LongColumnVector nodeChangeset = (LongColumnVector) nodeBatch.cols[0];
        LongColumnVector wayChangeset = (LongColumnVector) wayBatch.cols[0];
        LongColumnVector relationChangeset = (LongColumnVector) relationBatch.cols[0];
        LongColumnVector nodeId = (LongColumnVector) nodeBatch.cols[1];
        LongColumnVector wayId = (LongColumnVector) wayBatch.cols[1];
        LongColumnVector relationId = (LongColumnVector) relationBatch.cols[1];
        MapColumnVector nodeTags = (MapColumnVector) nodeBatch.cols[2];
        MapColumnVector wayTags = (MapColumnVector) wayBatch.cols[2];
        MapColumnVector relationTags = (MapColumnVector) relationBatch.cols[2];
        TimestampColumnVector nodeTimestamp = (TimestampColumnVector) nodeBatch.cols[3];
        TimestampColumnVector wayTimestamp = (TimestampColumnVector) wayBatch.cols[3];
        TimestampColumnVector relationTimestamp = (TimestampColumnVector) relationBatch.cols[3];
        LongColumnVector nodeUid = (LongColumnVector) nodeBatch.cols[4];
        LongColumnVector wayUid = (LongColumnVector) wayBatch.cols[4];
        LongColumnVector relationUid = (LongColumnVector) relationBatch.cols[4];
        BytesColumnVector nodeUser = (BytesColumnVector) nodeBatch.cols[5];
        BytesColumnVector wayUser = (BytesColumnVector) wayBatch.cols[5];
        BytesColumnVector relationUser = (BytesColumnVector) relationBatch.cols[5];
        LongColumnVector nodeVersion = (LongColumnVector) nodeBatch.cols[6];
        LongColumnVector wayVersion = (LongColumnVector) wayBatch.cols[6];
        LongColumnVector relationVersion = (LongColumnVector) relationBatch.cols[6];
        LongColumnVector nodeVisible = (LongColumnVector) nodeBatch.cols[7];
        LongColumnVector wayVisible = (LongColumnVector) wayBatch.cols[7];
        LongColumnVector relationVisible = (LongColumnVector) relationBatch.cols[7];
        DecimalColumnVector lat = (DecimalColumnVector) nodeBatch.cols[8];
        ListColumnVector nds = (ListColumnVector) wayBatch.cols[8];
        ListColumnVector members = (ListColumnVector) relationBatch.cols[8];
        DecimalColumnVector lon = (DecimalColumnVector) nodeBatch.cols[9];

        // TODO parallelize the processing of stuff produced by the iterator
        for (EntityContainer container : iterator) {
            OsmEntity entity = container.getEntity();
            OsmMetadata metadata = entity.getMetadata();

            int row;
            int i = 0;

            switch (container.getType()) {
                default:
                case Node:
                    row = nodeBatch.size++;

                    nodeChangeset.vector[row] = metadata.getChangeset();

                    nodeId.vector[row] = entity.getId();

                    nodeTags.offsets[row] = nodeTags.childCount;
                    nodeTags.lengths[row] = entity.getNumberOfTags(); // number of key/value pairings
                    nodeTags.childCount += nodeTags.lengths[row];
                    nodeTags.keys.ensureSize(nodeTags.childCount, nodeTags.offsets[row] != 0);
                    nodeTags.values.ensureSize(nodeTags.childCount, nodeTags.offsets[row] != 0);

                    for (Map.Entry<String, String> kv: OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                        ((BytesColumnVector) nodeTags.keys).setVal((int) nodeTags.offsets[row] + i, kv.getKey().getBytes());
                        ((BytesColumnVector) nodeTags.values).setVal((int) nodeTags.offsets[row] + i, kv.getValue().getBytes());

                        i++;
                    }

                    nodeTimestamp.time[row] = metadata.getTimestamp();
                    nodeTimestamp.nanos[row] = 0;

                    nodeUid.vector[row] = metadata.getUid();

                    nodeUser.setVal(row, metadata.getUser().getBytes());

                    nodeVersion.vector[row] = metadata.getVersion();
                    nodeVisible.vector[row] = 1;
                    
                    OsmNode node = (OsmNode) entity;

                    lat.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLatitude())));
                    lon.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLongitude())));

                    break;

                case Way:
                    row = wayBatch.size++;

                    wayChangeset.vector[row] = metadata.getChangeset();

                    wayId.vector[row] = entity.getId();

                    wayTags.offsets[row] = wayTags.childCount;
                    wayTags.lengths[row] = entity.getNumberOfTags(); // number of key/value pairings
                    wayTags.childCount += wayTags.lengths[row];
                    wayTags.keys.ensureSize(wayTags.childCount, wayTags.offsets[row] != 0);
                    wayTags.values.ensureSize(wayTags.childCount, wayTags.offsets[row] != 0);

                    for (Map.Entry<String, String> kv: OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                        ((BytesColumnVector) wayTags.keys).setVal((int) wayTags.offsets[row] + i, kv.getKey().getBytes());
                        ((BytesColumnVector) wayTags.values).setVal((int) wayTags.offsets[row] + i, kv.getValue().getBytes());

                        i++;
                    }

                    wayTimestamp.time[row] = metadata.getTimestamp();
                    wayTimestamp.nanos[row] = 0;

                    wayUid.vector[row] = metadata.getUid();

                    wayUser.setVal(row, metadata.getUser().getBytes());

                    wayVersion.vector[row] = metadata.getVersion();
                    wayVisible.vector[row] = 1;
                    
                    OsmWay way = (OsmWay) entity;

                    nds.offsets[row] = nds.childCount;
                    nds.lengths[row] = way.getNumberOfNodes();
                    nds.childCount += nds.lengths[row];
                    nds.child.ensureSize(nds.childCount, nds.offsets[row] != 0);

                    for (int j = 0; j < way.getNumberOfNodes(); j++) {
                        StructColumnVector ndsStruct = (StructColumnVector) nds.child;

                        ((LongColumnVector) ndsStruct.fields[0]).vector[(int) nds.offsets[row] + j] = way.getNodeId(j);
                    }

                    break;

                case Relation:
                    row = relationBatch.size++;

                    relationChangeset.vector[row] = metadata.getChangeset();

                    relationId.vector[row] = entity.getId();

                    relationTags.offsets[row] = relationTags.childCount;
                    relationTags.lengths[row] = entity.getNumberOfTags(); // number of key/value pairings
                    relationTags.childCount += relationTags.lengths[row];
                    relationTags.keys.ensureSize(relationTags.childCount, relationTags.offsets[row] != 0);
                    relationTags.values.ensureSize(relationTags.childCount, relationTags.offsets[row] != 0);

                    for (Map.Entry<String, String> kv: OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                        ((BytesColumnVector) relationTags.keys).setVal((int) relationTags.offsets[row] + i, kv.getKey().getBytes());
                        ((BytesColumnVector) relationTags.values).setVal((int) relationTags.offsets[row] + i, kv.getValue().getBytes());

                        i++;
                    }

                    relationTimestamp.time[row] = metadata.getTimestamp();
                    relationTimestamp.nanos[row] = 0;

                    relationUid.vector[row] = metadata.getUid();

                    relationUser.setVal(row, metadata.getUser().getBytes());

                    relationVersion.vector[row] = metadata.getVersion();
                    relationVisible.vector[row] = 1;

                    OsmRelation relation = (OsmRelation) entity;

                    members.offsets[row] = members.childCount;
                    members.lengths[row] = relation.getNumberOfMembers();
                    members.childCount += members.lengths[row];
                    members.child.ensureSize(members.childCount, members.offsets[row] != 0);

                    for (int j = 0; j < relation.getNumberOfMembers(); j++) {
                        StructColumnVector membersStruct = (StructColumnVector) members.child;
                        ((BytesColumnVector) membersStruct.fields[0]).setVal((int) members.offsets[row] + j, relation.getMember(j).getType().toString().toLowerCase().getBytes());
                        ((LongColumnVector) membersStruct.fields[1]).vector[(int) members.offsets[row] + j] = relation.getMember(j).getId();
                        ((BytesColumnVector) membersStruct.fields[2]).setVal((int) members.offsets[row] + j, relation.getMember(j).getRole().getBytes());
                    }

                    break;
            }

            // If the batch is full, write it out and start over.
            if (nodeBatch.size == nodeBatch.getMaxSize()) {
                nodeWriter.addRowBatch(nodeBatch);
                nodeBatch.reset();
            }
            if (wayBatch.size == wayBatch.getMaxSize()) {
                wayWriter.addRowBatch(wayBatch);
                wayBatch.reset();
            }
            if (relationBatch.size == relationBatch.getMaxSize()) {
                relationWriter.addRowBatch(relationBatch);
                relationBatch.reset();
            }
        }

        nodeWriter.close();
        wayWriter.close();
        relationWriter.close();
    }
}
