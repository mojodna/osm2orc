package net.mojodna.osm2orc.standalone;


import de.topobyte.osm4j.core.access.OsmIterator;
import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import de.topobyte.osm4j.core.model.iface.OsmMetadata;
import de.topobyte.osm4j.core.model.iface.OsmNode;
import de.topobyte.osm4j.core.model.iface.OsmRelation;
import de.topobyte.osm4j.core.model.iface.OsmWay;
import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.orc.TypeDescription.createBoolean;
import static org.apache.orc.TypeDescription.createDecimal;
import static org.apache.orc.TypeDescription.createList;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createMap;
import static org.apache.orc.TypeDescription.createString;
import static org.apache.orc.TypeDescription.createStruct;
import static org.apache.orc.TypeDescription.createTimestamp;

public class OsmPbf2Orc {
    private static final Logger LOG = Logger.getLogger(OsmPbf2Orc.class.getName());
    private static final byte[] NODE_BYTES = "node".getBytes();
    private static final byte[] WAY_BYTES = "way".getBytes();
    private static final byte[] RELATION_BYTES = "relation".getBytes();

    public static void convert(InputStream input, String outputOrc, boolean history) throws Exception {
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

        if (history) {
            schema.addField("visible", createBoolean());
        }

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


        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector id = (LongColumnVector) batch.cols[0];
        BytesColumnVector type = (BytesColumnVector) batch.cols[1];
        MapColumnVector tags = (MapColumnVector) batch.cols[2];
        DecimalColumnVector lat = (DecimalColumnVector) batch.cols[3];
        DecimalColumnVector lon = (DecimalColumnVector) batch.cols[4];
        ListColumnVector nds = (ListColumnVector) batch.cols[5];
        StructColumnVector ndsStruct = (StructColumnVector) nds.child;
        ListColumnVector members = (ListColumnVector) batch.cols[6];
        StructColumnVector membersStruct = (StructColumnVector) members.child;
        LongColumnVector changeset = (LongColumnVector) batch.cols[7];
        TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[8];
        LongColumnVector uid = (LongColumnVector) batch.cols[9];
        BytesColumnVector user = (BytesColumnVector) batch.cols[10];
        LongColumnVector version = (LongColumnVector) batch.cols[11];
        final LongColumnVector visible;

        if (history) {
            visible = (LongColumnVector) batch.cols[12];
        } else {
            visible = null;
        }

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

            synchronized (tags) {
                tags.offsets[row] = tags.childCount;
                tags.lengths[row] = entity.getNumberOfTags(); // number of key/value pairings
                tags.childCount += tags.lengths[row];
                tags.keys.ensureSize(tags.childCount, tags.offsets[row] != 0);
                tags.values.ensureSize(tags.childCount, tags.offsets[row] != 0);

                int i = 0;
                for (Map.Entry<String, String> kv : OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                    byte[] key = kv.getKey().getBytes();
                    byte[] val = kv.getValue().getBytes();
                    ((BytesColumnVector) tags.keys).setRef((int) tags.offsets[row] + i, key, 0, key.length);
                    ((BytesColumnVector) tags.values).setRef((int) tags.offsets[row] + i, val, 0, val.length);

                    ++i;
                }
            }

            timestamp.time[row] = metadata.getTimestamp();
            timestamp.nanos[row] = 0;

            uid.vector[row] = metadata.getUid();

            byte[] userBytes = metadata.getUser().getBytes();
            user.setRef(row, userBytes, 0, userBytes.length);

            version.vector[row] = metadata.getVersion();

            if (history) {
                visible.vector[row] = 1;
                if (!metadata.isVisible()) {
                    visible.vector[row] = 0;
                }
            }

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
                    type.setRef(row, NODE_BYTES, 0, NODE_BYTES.length);

                    OsmNode node = (OsmNode) entity;

                    lat.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLatitude())));
                    lon.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLongitude())));

                    break;

                case Way:
                    type.setRef(row, WAY_BYTES, 0, WAY_BYTES.length);

                    lat.set(row, (HiveDecimal) null);
                    lon.set(row, (HiveDecimal) null);

                    OsmWay way = (OsmWay) entity;

                    synchronized (nds) {
                        nds.lengths[row] = way.getNumberOfNodes();
                        nds.childCount += nds.lengths[row];
                        ndsStruct.ensureSize(nds.childCount, nds.offsets[row] != 0);

                        for (int j = 0; j < way.getNumberOfNodes(); j++) {
                            ((LongColumnVector) ndsStruct.fields[0]).vector[(int) nds.offsets[row] + j] = way.getNodeId(j);
                        }
                    }

                    break;

                case Relation:
                    type.setRef(row, RELATION_BYTES, 0, RELATION_BYTES.length);

                    lat.set(row, (HiveDecimal) null);
                    lon.set(row, (HiveDecimal) null);

                    OsmRelation relation = (OsmRelation) entity;

                    synchronized (members) {
                        members.lengths[row] = relation.getNumberOfMembers();
                        members.childCount += members.lengths[row];
                        membersStruct.ensureSize(members.childCount, members.offsets[row] != 0);

                        for (int j = 0; j < relation.getNumberOfMembers(); j++) {
                            final byte[] typeBytes;
                            switch (relation.getMember(j).getType()) {
                            case Node:
                                typeBytes = NODE_BYTES;
                                break;

                            case Way:
                                typeBytes = WAY_BYTES;
                                break;

                            case Relation:
                                typeBytes = RELATION_BYTES;
                                break;

                            default:
                                throw new RuntimeException("Unsupported member type: " + relation.getMember(j).getType());
                            }

                            ((BytesColumnVector) membersStruct.fields[0]).setRef((int) members.offsets[row] + j, typeBytes, 0, typeBytes.length);
                            ((LongColumnVector) membersStruct.fields[1]).vector[(int) members.offsets[row] + j] = relation.getMember(j).getId();

                            byte[] roleBytes = relation.getMember(j).getRole().getBytes();
                            ((BytesColumnVector) membersStruct.fields[2]).setRef((int) members.offsets[row] + j, roleBytes, 0, roleBytes.length);
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
