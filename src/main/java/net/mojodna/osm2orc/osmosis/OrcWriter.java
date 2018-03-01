package net.mojodna.osm2orc.osmosis;

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
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityProcessor;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.logging.Logger;

import static org.apache.orc.TypeDescription.createBoolean;
import static org.apache.orc.TypeDescription.createDecimal;
import static org.apache.orc.TypeDescription.createList;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createMap;
import static org.apache.orc.TypeDescription.createString;
import static org.apache.orc.TypeDescription.createStruct;
import static org.apache.orc.TypeDescription.createTimestamp;

public class OrcWriter implements Sink {
    private static final Logger LOG = Logger.getLogger(OrcWriter.class.getName());

    private static final TypeDescription SCHEMA = createStruct()
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
            .addField("version", createLong())
            .addField("visible", createBoolean());

    private OrcEntityProcessor processor;
    private String filename;

    private class OrcEntityProcessor implements EntityProcessor {
        private final Writer writer;
        private final VectorizedRowBatch batch;
        private int row;

        OrcEntityProcessor(Writer writer, VectorizedRowBatch batch) {
            this.writer = writer;
            this.batch = batch;
        }

        private void checkLimit() {
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                    batch.reset();
                } catch (IOException e) {
                    throw new OsmosisRuntimeException(e);
                }
            }

            row = batch.size++;
        }

        private void addCommonProperties(EntityContainer container) {
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            BytesColumnVector type = (BytesColumnVector) batch.cols[1];
            MapColumnVector tags = (MapColumnVector) batch.cols[2];
            ListColumnVector nds = (ListColumnVector) batch.cols[5];
            ListColumnVector members = (ListColumnVector) batch.cols[6];
            LongColumnVector changeset = (LongColumnVector) batch.cols[7];
            TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[8];
            LongColumnVector uid = (LongColumnVector) batch.cols[9];
            BytesColumnVector user = (BytesColumnVector) batch.cols[10];
            LongColumnVector version = (LongColumnVector) batch.cols[11];
            LongColumnVector visible = (LongColumnVector) batch.cols[12];

            Entity entity = container.getEntity();

            id.vector[row] = entity.getId();
            changeset.vector[row] = entity.getChangesetId();
            type.setVal(row, entity.getType().toString().toLowerCase().getBytes());

            tags.offsets[row] = tags.childCount;
            tags.lengths[row] = entity.getTags().size(); // number of key/value pairings
            tags.childCount += tags.lengths[row];
            tags.keys.ensureSize(tags.childCount, tags.offsets[row] != 0);
            tags.values.ensureSize(tags.childCount, tags.offsets[row] != 0);

            int i = 0;
            for (Tag tag : entity.getTags()) {
                ((BytesColumnVector) tags.keys).setVal((int) tags.offsets[row] + i, tag.getKey().getBytes());
                ((BytesColumnVector) tags.values).setVal((int) tags.offsets[row] + i, tag.getValue().getBytes());

                i++;
            }

            timestamp.time[row] = entity.getTimestamp().getTime();
            timestamp.nanos[row] = 0;

            uid.vector[row] = entity.getUser().getId();

            user.setVal(row, entity.getUser().getName().getBytes());

            version.vector[row] = entity.getVersion();

            visible.vector[row] = 1;
            if (entity.getMetaTags().get("visible") == Boolean.FALSE) {
                visible.vector[row] = 0;
            }

            nds.offsets[row] = nds.childCount;
            nds.lengths[row] = 0;

            members.offsets[row] = members.childCount;
            members.lengths[row] = 0;
        }

        @Override
        public void process(BoundContainer bound) {
            // TODO set bounds in metadata
        }

        @Override
        public void process(NodeContainer container) {
            DecimalColumnVector lat = (DecimalColumnVector) batch.cols[3];
            DecimalColumnVector lon = (DecimalColumnVector) batch.cols[4];

            checkLimit();
            addCommonProperties(container);

            Node node = container.getEntity();
            lat.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLatitude())));
            lon.set(row, HiveDecimal.create(BigDecimal.valueOf(node.getLongitude())));
        }

        @Override
        public void process(WayContainer container) {
            DecimalColumnVector lat = (DecimalColumnVector) batch.cols[3];
            DecimalColumnVector lon = (DecimalColumnVector) batch.cols[4];
            ListColumnVector nds = (ListColumnVector) batch.cols[5];

            checkLimit();
            addCommonProperties(container);

            lat.isNull[row] = true;
            lon.isNull[row] = true;
            lat.set(row, (HiveDecimal) null);
            lon.set(row, (HiveDecimal) null);

            Way way = container.getEntity();

            nds.lengths[row] = way.getWayNodes().size();
            nds.childCount += nds.lengths[row];
            nds.child.ensureSize(nds.childCount, nds.offsets[row] != 0);

            for (int j = 0; j < way.getWayNodes().size(); j++) {
                StructColumnVector ndsStruct = (StructColumnVector) nds.child;

                ((LongColumnVector) ndsStruct.fields[0]).vector[(int) nds.offsets[row] + j] = way.getWayNodes().get(j).getNodeId();
            }
        }

        @Override
        public void process(RelationContainer container) {
            DecimalColumnVector lat = (DecimalColumnVector) batch.cols[3];
            DecimalColumnVector lon = (DecimalColumnVector) batch.cols[4];
            ListColumnVector members = (ListColumnVector) batch.cols[6];

            checkLimit();
            addCommonProperties(container);

            lat.isNull[row] = true;
            lon.isNull[row] = true;
            lat.set(row, (HiveDecimal) null);
            lon.set(row, (HiveDecimal) null);

            Relation relation = container.getEntity();

            members.lengths[row] = relation.getMembers().size();
            members.childCount += members.lengths[row];
            members.child.ensureSize(members.childCount, members.offsets[row] != 0);

            for (int j = 0; j < relation.getMembers().size(); j++) {
                StructColumnVector membersStruct = (StructColumnVector) members.child;

                ((BytesColumnVector) membersStruct.fields[0]).setVal((int) members.offsets[row] + j, relation.getMembers().get(j).getMemberType().toString().toLowerCase().getBytes());
                ((LongColumnVector) membersStruct.fields[1]).vector[(int) members.offsets[row] + j] = relation.getMembers().get(j).getMemberId();
                ((BytesColumnVector) membersStruct.fields[2]).setVal((int) members.offsets[row] + j, relation.getMembers().get(j).getMemberRole().getBytes());
            }
        }

        void flush() throws IOException {
            writer.addRowBatch(batch);
        }

        void close() throws IOException {
            flush();
            writer.close();
        }
    }

    OrcWriter(String filename) {
        this.filename = filename;
    }

    @Override
    public void process(EntityContainer entityContainer) {
        entityContainer.process(processor);
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        try {
            Configuration conf = new Configuration();
            // conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags");
            processor = new OrcEntityProcessor(OrcFile.createWriter(new Path(filename),
                    OrcFile.writerOptions(conf).setSchema(SCHEMA)), SCHEMA.createRowBatch());
        } catch (IOException e) {
            throw new OsmosisRuntimeException(e);
        }
    }

    @Override
    public void complete() {
        try {
            // flush any pending rows
            processor.close();
            processor = null;
        } catch (IOException e) {
            throw new OsmosisRuntimeException("Unable to complete the ORC file.", e);
        }
    }

    @Override
    public void close() {
    }
}
