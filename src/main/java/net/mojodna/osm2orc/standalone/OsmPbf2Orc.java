package net.mojodna.osm2orc.standalone;


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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.orc.TypeDescription.*;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createString;

public class OsmPbf2Orc {
    public static void convert(String inputPbf, String outputOrc) throws Exception {
        TypeDescription schema = createStruct()
                .addField("id", createLong())
                .addField("type", createString())
                .addField("tags", createMap(
                        createString(),
                        createString()
                ))
                .addField("lat", createDecimal().withScale(7).withPrecision(9))
                .addField("lon", createDecimal().withScale(7).withPrecision(10))
                .addField("nds", createList(
                        createStruct()
                                .addField("ref", createLong())
                ))
                .addField("members", createList(
                        createStruct()
                                .addField("type", createString())
                                .addField("ref", createLong())
                                .addField("role", createString())
                ))
                .addField("changeset", createLong())
                .addField("timestamp", createTimestamp())
                .addField("uid", createLong())
                .addField("user", createString())
                .addField("version", createLong());

//        TypeDescription deltaSchema = createStruct()
//                .addField("operation", createInt())
//                // transaction id of the base
//                .addField("originalTransaction", createLong())
//                // bucket after hashing the column being clustered
//                .addField("bucket", createInt())
//                // index into entire dataset (row in stripe + row base in stripe + first row)
//                // could also be initialized when writing and incremented when creating each row (like row)
//                .addField("rowId", createLong())
//                // delta transaction id
//                .addField("currentTransaction", createLong())
//                .addField("row", schema);


        Configuration conf = new Configuration();
//        conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags");
        Writer writer = OrcFile.createWriter(new Path(outputOrc),
                OrcFile.writerOptions(conf).setSchema(schema));

//        writer.addUserMetadata("bbox", null);
//        // TODO osm.schema.version = 0.6
//        writer.addUserMetadata("OsmSchema-V0.6", null);
//        writer.addUserMetadata("required_features", null);
//        writer.addUserMetadata("HistoricalInformation", null);
//        writer.addUserMetadata("has_metadata", null);
//        writer.addUserMetadata("Sort.Type_then_ID", null);
//        writer.addUserMetadata("Sort.Geographic", null);
//        // see "What are the replication fields for?" in https://wiki.openstreetmap.org/wiki/PBF_Format
//        writer.addUserMetadata("replication_timestamp", null);
//        writer.addUserMetadata("replication_sequence_number", null);
//        writer.addUserMetadata("replication_base_url", null);
//        // to allow conversion of decimals into ints and avoid hard-coding the precision
//        writer.addUserMetadata("granularity", null);
//        // false easting
//        writer.addUserMetadata("x_offset", null);
//        // false northing
//        writer.addUserMetadata("y_offset", null);


        InputStream input = new FileInputStream(inputPbf);

        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector id = (LongColumnVector) batch.cols[0];
        BytesColumnVector type = (BytesColumnVector) batch.cols[1];
        MapColumnVector tags = (MapColumnVector) batch.cols[2];
        DecimalColumnVector lat = (DecimalColumnVector) batch.cols[3];
        DecimalColumnVector lon = (DecimalColumnVector) batch.cols[4];
        ListColumnVector nds = (ListColumnVector) batch.cols[5];
        ListColumnVector members = (ListColumnVector) batch.cols[6];
        LongColumnVector changeset = (LongColumnVector) batch.cols[7];
        TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[8];
        LongColumnVector uid = (LongColumnVector) batch.cols[9];
        BytesColumnVector user = (BytesColumnVector) batch.cols[10];
        LongColumnVector version = (LongColumnVector) batch.cols[11];

        OsmIterator iterator = new PbfIterator(input, true);
        // parallel will make it faster but will produce bigger output files
        Stream<EntityContainer> entityStream = StreamSupport.stream(iterator.spliterator(), false);

        entityStream.forEach(container -> {
            OsmEntity entity = container.getEntity();
            OsmMetadata metadata = entity.getMetadata();

            int row;

            synchronized (batch) {
                if (batch.size == batch.getMaxSize()) {
                    try {
                        writer.addRowBatch(batch);
                        batch.reset();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                row = batch.size++;
            }

            id.vector[row] = entity.getId();
            changeset.vector[row] = metadata.getChangeset();
            type.setVal(row, container.getType().toString().toLowerCase().getBytes());

            synchronized (tags) {
                tags.offsets[row] = tags.childCount;
                tags.lengths[row] = entity.getNumberOfTags(); // number of key/value pairings
                tags.childCount += tags.lengths[row];
                tags.keys.ensureSize(tags.childCount, tags.offsets[row] != 0);
                tags.values.ensureSize(tags.childCount, tags.offsets[row] != 0);

                int i = 0;
                for (Map.Entry<String, String> kv : OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                    ((BytesColumnVector) tags.keys).setVal((int) tags.offsets[row] + i, kv.getKey().getBytes());
                    ((BytesColumnVector) tags.values).setVal((int) tags.offsets[row] + i, kv.getValue().getBytes());

                    ++i;
                }
            }

            timestamp.time[row] = metadata.getTimestamp();
            timestamp.nanos[row] = 0;

            uid.vector[row] = metadata.getUid();

            user.setVal(row, metadata.getUser().getBytes());

            version.vector[row] = metadata.getVersion();

            synchronized (nds) {
                nds.offsets[row] = nds.childCount;
                nds.lengths[row] = 0;
            }

            synchronized (members) {
                members.offsets[row] = members.childCount;
                members.lengths[row] = 0;
            }

            // TODO changeset, in which case lat/lon need to be zeroed out
            // changesets also include discussion, which is a list of comments (date, uid, user, text)
            // changesets can be open/closed, have a created_at (same as timestamp?), and a bbox (4 values)
            switch (container.getType()) {
                default:
                case Node:
                    OsmNode node = (OsmNode) entity;

                    lat.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLatitude())));
                    lon.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLongitude())));

                    break;

                case Way:
                    lat.set(row, (HiveDecimal) null);
                    lon.set(row, (HiveDecimal) null);

                    OsmWay way = (OsmWay) entity;

                    synchronized (nds) {
                        nds.lengths[row] = way.getNumberOfNodes();
                        nds.childCount += nds.lengths[row];
                        nds.child.ensureSize(nds.childCount, nds.offsets[row] != 0);

                        for (int j = 0; j < way.getNumberOfNodes(); j++) {
                            StructColumnVector ndsStruct = (StructColumnVector) nds.child;

                            ((LongColumnVector) ndsStruct.fields[0]).vector[(int) nds.offsets[row] + j] = way.getNodeId(j);
                        }
                    }

                    break;

                case Relation:
                    lat.set(row, (HiveDecimal) null);
                    lon.set(row, (HiveDecimal) null);

                    OsmRelation relation = (OsmRelation) entity;

                    synchronized (members) {
                        members.lengths[row] = relation.getNumberOfMembers();
                        members.childCount += members.lengths[row];
                        members.child.ensureSize(members.childCount, members.offsets[row] != 0);

                        for (int j = 0; j < relation.getNumberOfMembers(); j++) {
                            StructColumnVector membersStruct = (StructColumnVector) members.child;

                            ((BytesColumnVector) membersStruct.fields[0]).setVal((int) members.offsets[row] + j, relation.getMember(j).getType().toString().toLowerCase().getBytes());
                            ((LongColumnVector) membersStruct.fields[1]).vector[(int) members.offsets[row] + j] = relation.getMember(j).getId();
                            ((BytesColumnVector) membersStruct.fields[2]).setVal((int) members.offsets[row] + j, relation.getMember(j).getRole().getBytes());
                        }
                    }

                    break;
            }
        });

        // flush any pending rows
        writer.addRowBatch(batch);
        writer.close();
    }
}
