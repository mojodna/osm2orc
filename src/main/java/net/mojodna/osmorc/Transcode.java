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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.orc.TypeDescription.*;

public class Transcode {
    public static void main(String[] args) throws Exception {
        TypeDescription schema = createStruct()
                .addField("id", createLong())
                .addField("type", createString())
                .addField("changeset", createLong())
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
                ));

        Configuration conf = new Configuration();
        // id can be omitted if elements are assumed to be sorted
        // user can be omitted if separate user mappings are kept (to produce a list of uids from a list of usernames)
//        conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags,type");
        Writer writer = OrcFile.createWriter(new Path("osm.orc"),
                OrcFile.writerOptions(conf).setSchema(schema));

        InputStream input = new FileInputStream(args[0]);

        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector id = (LongColumnVector) batch.cols[0];
        BytesColumnVector type = (BytesColumnVector) batch.cols[1];
        LongColumnVector changeset = (LongColumnVector) batch.cols[2];
        MapColumnVector tags = (MapColumnVector) batch.cols[3];
        TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[4];
        LongColumnVector uid = (LongColumnVector) batch.cols[5];
        BytesColumnVector user = (BytesColumnVector) batch.cols[6];
        LongColumnVector version = (LongColumnVector) batch.cols[7];
        LongColumnVector visible = (LongColumnVector) batch.cols[8];
        DecimalColumnVector lat = (DecimalColumnVector) batch.cols[9];
        DecimalColumnVector lon = (DecimalColumnVector) batch.cols[10];
        ListColumnVector nds = (ListColumnVector) batch.cols[11];
        ListColumnVector members = (ListColumnVector) batch.cols[12];

        OsmIterator iterator = new PbfIterator(input, true);
        Stream<EntityContainer> entityStream = StreamSupport.stream(iterator.spliterator(), true);

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
            }

            synchronized (tags) {
                int i = 0;
                for (Map.Entry<String, String> kv : OsmModelUtil.getTagsAsMap(entity).entrySet()) {
                    ((BytesColumnVector) tags.keys).setVal((int) tags.offsets[row] + i, kv.getKey().getBytes());
                    ((BytesColumnVector) tags.values).setVal((int) tags.offsets[row] + i, kv.getValue().getBytes());

                    i++;
                }
            }

            timestamp.time[row] = metadata.getTimestamp();
            timestamp.nanos[row] = 0;

            uid.vector[row] = metadata.getUid();

            user.setVal(row, metadata.getUser().getBytes());

            version.vector[row] = metadata.getVersion();
            visible.vector[row] = 1;

            switch (container.getType()) {
                default:
                case Node:
                    OsmNode node = (OsmNode) entity;

                    lat.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLatitude())));
                    lon.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLongitude())));

                    break;

                case Way:
                    OsmWay way = (OsmWay) entity;

                    synchronized (nds) {
                        nds.offsets[row] = nds.childCount;
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
                    OsmRelation relation = (OsmRelation) entity;

                    synchronized (members) {
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
                    }

                    break;
            }
        });

        // flush any pending rows
        writer.addRowBatch(batch);
        writer.close();
    }
}
