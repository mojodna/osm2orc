package net.mojodna.osm2orc.standalone;


import net.mojodna.osm2orc.standalone.model.Changeset;
import net.mojodna.osm2orc.standalone.parser.ChangesetXmlHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.orc.TypeDescription.createBoolean;
import static org.apache.orc.TypeDescription.createDecimal;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createMap;
import static org.apache.orc.TypeDescription.createString;
import static org.apache.orc.TypeDescription.createStruct;
import static org.apache.orc.TypeDescription.createTimestamp;

public class OsmChangesetXml2Orc {
    private static final TypeDescription SCHEMA = createStruct()
            .addField(Changeset.ID, createLong())
            .addField("tags", createMap(
                    createString(),
                    createString()
            ))
            .addField(Changeset.CREATED_AT, createTimestamp())
            .addField(Changeset.OPEN, createBoolean())
            .addField(Changeset.CLOSED_AT, createTimestamp())
            .addField(Changeset.COMMENTS_COUNT, createLong())
            .addField(Changeset.MIN_LAT, createDecimal().withScale(7).withPrecision(9))
            .addField(Changeset.MAX_LAT, createDecimal().withScale(7).withPrecision(9))
            .addField(Changeset.MIN_LON, createDecimal().withScale(7).withPrecision(10))
            .addField(Changeset.MAX_LON, createDecimal().withScale(7).withPrecision(10))
            .addField(Changeset.NUM_CHANGES, createLong())
            .addField(Changeset.UID, createLong())
            .addField(Changeset.USER, createString());

    private String inputChangesetXml;
    private String outputOrc;

    public OsmChangesetXml2Orc(String inputChangesetXml, String outputOrc) {
        this.inputChangesetXml = inputChangesetXml;
        this.outputOrc = outputOrc;
    }

    public void convert() throws Exception {
        // Setup ORC writer
        Configuration conf = new Configuration();
        Writer writer = OrcFile.createWriter(new Path(outputOrc),
                OrcFile.writerOptions(conf).setSchema(SCHEMA));

        // Setup ORC vectors
        VectorizedRowBatch batch = SCHEMA.createRowBatch();
        LongColumnVector id = (LongColumnVector) batch.cols[0];
        MapColumnVector tags = (MapColumnVector) batch.cols[1];
        TimestampColumnVector createdAt = (TimestampColumnVector) batch.cols[2];
        LongColumnVector open = (LongColumnVector) batch.cols[3];
        TimestampColumnVector closedAt = (TimestampColumnVector) batch.cols[4];
        LongColumnVector commentsCount = (LongColumnVector) batch.cols[5];
        DecimalColumnVector minLat = (DecimalColumnVector) batch.cols[6];
        DecimalColumnVector maxLat = (DecimalColumnVector) batch.cols[7];
        DecimalColumnVector minLon = (DecimalColumnVector) batch.cols[8];
        DecimalColumnVector maxLon = (DecimalColumnVector) batch.cols[9];
        LongColumnVector numChanges = (LongColumnVector) batch.cols[10];
        LongColumnVector uid = (LongColumnVector) batch.cols[11];
        BytesColumnVector user = (BytesColumnVector) batch.cols[12];

        // Parse Changeset XML
        InputStream inputStream = new FileInputStream(inputChangesetXml);
        SAXParser parser = SAXParserFactory.newInstance().newSAXParser();

        final AtomicLong count = new AtomicLong(0);

        parser.parse(inputStream, new ChangesetXmlHandler(changeset -> {
            int row;
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                    batch.reset();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            row = batch.size++;
            id.vector[row] = changeset.getId();

            try {
                createdAt.time[row] = changeset.getCreatedAt().getTimestamp().getTime();
            } catch (Exception e) {
                createdAt.time[row] = 0;
                createdAt.isNull[row] = true;
            }
            createdAt.nanos[row] = 0;

            try {
                closedAt.time[row] = changeset.getClosedAt().getTimestamp().getTime();
            } catch (Exception e) {
                closedAt.time[row] = 0;
                closedAt.isNull[row] = true;
            }
            closedAt.nanos[row] = 0;

            if (changeset.isOpen()) {
                open.vector[row] = 1;
            } else {
                open.vector[row] = 0;
            }
            numChanges.vector[row] = changeset.getNumChanges();

            if (changeset.getUser() != null) {
                user.setVal(row, changeset.getUser().getBytes());
            } else {
                user.setVal(row, new byte[0]);
                user.isNull[row] = true;
            }
            if (changeset.getUid() != null) {
                uid.vector[row] = changeset.getUid();
            } else {
                uid.isNull[row] = true;
            }

            // We've kept these parsed values as string
            // to guarantee no double precision loss.
            String minLatStr = changeset.getMinLat();
            String maxLatStr = changeset.getMaxLat();
            String minLonStr = changeset.getMinLon();
            String maxLonStr = changeset.getMaxLon();
            if (minLatStr != null) {
                minLat.set(row, HiveDecimal.create(new BigDecimal(maxLatStr)));
            } else {
                minLat.set(row, (HiveDecimal) null);
                minLat.isNull[row] = true;
            }
            if (maxLatStr != null) {
                maxLat.set(row, HiveDecimal.create(new BigDecimal(maxLatStr)));
            } else {
                maxLat.set(row, (HiveDecimal) null);
                maxLat.isNull[row] = true;
            }
            if (minLonStr != null) {
                minLon.set(row, HiveDecimal.create(new BigDecimal(minLonStr)));
            } else {
                minLon.set(row, (HiveDecimal) null);
                minLon.isNull[row] = true;
            }
            if (maxLonStr != null) {
                maxLon.set(row, HiveDecimal.create(new BigDecimal(maxLonStr)));
            } else {
                maxLon.set(row, (HiveDecimal) null);
                maxLon.isNull[row] = true;
            }
            commentsCount.vector[row] = changeset.getCommentsCount();

            // tags
            tags.offsets[row] = tags.childCount;
            Map<String,String> _tags = changeset.getTags();
            tags.lengths[row] = _tags.size();
            tags.childCount += tags.lengths[row];
            tags.keys.ensureSize(tags.childCount, tags.offsets[row] != 0);
            tags.values.ensureSize(tags.childCount, tags.offsets[row] != 0);
            int i = 0;
            for (Map.Entry<String, String> kv : _tags.entrySet()) {
                ((BytesColumnVector) tags.keys).setVal((int) tags.offsets[row] + i, kv.getKey().getBytes());
                ((BytesColumnVector) tags.values).setVal((int) tags.offsets[row] + i, kv.getValue().getBytes());
                ++i;
            }
            System.out.println(count.incrementAndGet() + " changesets converted to orc. id = "
                    + changeset.getId() + " user=" + changeset.getUser());
        }));

        // flush any pending rows
        writer.addRowBatch(batch);
        writer.close();
    }
}
