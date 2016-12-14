package net.mojodna.osm2orc.standalone;


import net.mojodna.osm2orc.standalone.model.Changeset;
import net.mojodna.osm2orc.standalone.parser.ChangesetXmlHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
        List<Changeset> changesets = new ArrayList<>();
        parser.parse(inputStream, new ChangesetXmlHandler(changesets));

        // TODO We need to stream these changesets so we don't have them all in memory...
        for (Changeset changeset : changesets) {

        }
    }
}
