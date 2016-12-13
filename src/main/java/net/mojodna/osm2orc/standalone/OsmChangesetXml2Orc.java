package net.mojodna.osm2orc.standalone;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.FileInputStream;
import java.io.InputStream;

import static org.apache.orc.TypeDescription.*;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createString;

public class OsmChangesetXml2Orc {

    private static final TypeDescription SCHEMA = createStruct()
            .addField("id", createLong())
            .addField("created_at", createTimestamp())
            .addField("closed_at", createTimestamp())
            .addField("open", createBoolean())
            .addField("num_changes", createLong())
            .addField("user", createString())
            .addField("uid", createLong())
            .addField("min_lat", createDecimal().withScale(7).withPrecision(9))
            .addField("max_lat", createDecimal().withScale(7).withPrecision(9))
            .addField("min_lon", createDecimal().withScale(7).withPrecision(10))
            .addField("max_lon", createDecimal().withScale(7).withPrecision(10))
            .addField("comments_count", createLong())
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
        Configuration conf = new Configuration();
        Writer writer = OrcFile.createWriter(new Path(outputOrc),
                OrcFile.writerOptions(conf).setSchema(SCHEMA));
        InputStream input = new FileInputStream(inputChangesetXml);

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


    }
}
