package net.mojodna.osm2orc.standalone;


import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import net.mojodna.osm2orc.standalone.model.Changeset;
import net.mojodna.osm2orc.standalone.parser.ChangesetXmlHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.orc.TypeDescription.*;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createString;

public class OsmChangesetXml2Orc {

    private static final TypeDescription SCHEMA = createStruct()
            .addField(Changeset.ID, createLong())
            .addField(Changeset.CREATED_AT, createTimestamp())
            .addField(Changeset.CLOSED_AT, createTimestamp())
            .addField(Changeset.OPEN, createBoolean())
            .addField(Changeset.NUM_CHANGES, createLong())
            .addField(Changeset.USER, createString())
            .addField(Changeset.UID, createLong())
            .addField(Changeset.MIN_LAT, createDecimal().withScale(7).withPrecision(9))
            .addField(Changeset.MAX_LAT, createDecimal().withScale(7).withPrecision(9))
            .addField(Changeset.MIN_LON, createDecimal().withScale(7).withPrecision(10))
            .addField(Changeset.MAX_LON, createDecimal().withScale(7).withPrecision(10))
            .addField(Changeset.COMMENTS_COUNT, createLong())
            .addField("tags", createMap(
                    createString(),
                    createString()
            ));

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
        TimestampColumnVector created_at = (TimestampColumnVector) batch.cols[1];
        TimestampColumnVector closed_at = (TimestampColumnVector) batch.cols[2];
        LongColumnVector open = (LongColumnVector) batch.cols[3];
        LongColumnVector num_changes = (LongColumnVector) batch.cols[4];
        BytesColumnVector user = (BytesColumnVector) batch.cols[5];
        LongColumnVector uid = (LongColumnVector) batch.cols[6];
        DecimalColumnVector min_lat = (DecimalColumnVector) batch.cols[7];
        DecimalColumnVector max_lat = (DecimalColumnVector) batch.cols[8];
        DecimalColumnVector min_lon = (DecimalColumnVector) batch.cols[9];
        DecimalColumnVector max_lon = (DecimalColumnVector) batch.cols[10];
        LongColumnVector comments_count = (LongColumnVector) batch.cols[11];
        MapColumnVector tags = (MapColumnVector) batch.cols[12];

        // Parse Changeset XML
        InputStream inputStream = new FileInputStream(inputChangesetXml);
        SAXParser parser = SAXParserFactory.newInstance().newSAXParser();

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
                created_at.time[row] = changeset.getCreated_at().getTimestamp().getTime();
            } catch (Exception e) {
                created_at.time[row] = 0;
            }
            created_at.nanos[row] = 0;

            try {
                closed_at.time[row] = changeset.getClosed_at().getTimestamp().getTime();
            } catch (Exception e) {
                closed_at.time[row] = 0;
            }
            closed_at.nanos[row] = 0;

            if (changeset.isOpen()) {
                open.vector[row] = 1;
            } else {
                open.vector[row] = 0;
            }
            num_changes.vector[row] = changeset.getNum_changes();

            if (changeset.getUser() != null) {
                user.setVal(row, changeset.getUser().getBytes());
            } else {
                user.setVal(row, new byte[0]);
            }
            if (changeset.getUid() != null) {
                uid.vector[row] = changeset.getUid();
            } else {
                uid.vector[row] = -1;
            }


            Double minLat = changeset.getMin_lat();
            Double maxLat = changeset.getMax_lat();
            Double minLon = changeset.getMin_lon();
            Double maxLon = changeset.getMax_lon();
            if (minLat != null) {
                min_lat.set(row, HiveDecimal.create(BigDecimal.valueOf(minLat)));
            } else {
                min_lat.set(row, (HiveDecimal) null);
            }
            if (maxLat != null) {
                max_lat.set(row, HiveDecimal.create(BigDecimal.valueOf(maxLat)));
            } else {
                max_lat.set(row, (HiveDecimal) null);
            }
            if (minLon != null) {
                min_lon.set(row, HiveDecimal.create(BigDecimal.valueOf(minLon)));
            } else {
                min_lon.set(row, (HiveDecimal) null);
            }
            if (maxLon != null) {
                max_lon.set(row, HiveDecimal.create(BigDecimal.valueOf(maxLon)));
            } else {
                max_lon.set(row, (HiveDecimal) null);
            }
            comments_count.vector[row] = changeset.getComments_count();

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
                i++;
            }

            System.out.println("Converted " + changeset.instanceCount() + " changesets to orc. id = " + changeset.getId());
        }));

        // flush any pending rows
        writer.addRowBatch(batch);
        writer.close();

    }
}
