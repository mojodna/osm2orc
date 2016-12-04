package net.mojodna.osmorc;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.*;

import static org.apache.orc.TypeDescription.createBoolean;
import static org.apache.orc.TypeDescription.createDecimal;
import static org.apache.orc.TypeDescription.createList;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createMap;
import static org.apache.orc.TypeDescription.createString;
import static org.apache.orc.TypeDescription.createStruct;
import static org.apache.orc.TypeDescription.createTimestamp;
import static org.apache.orc.TypeDescription.createUnion;

import java.io.IOException;
import java.sql.Timestamp;

public class WriteFileExample {
    public static void main(String[] args) throws IOException {
        TypeDescription schema = createStruct()
                .addField("changeset", createLong())
                .addField("id", createLong())
                .addField("tags", createMap(
                        createString(),
                        createString()
                ))
                .addField("timestamp", createTimestamp())
                .addField("type", createString())
                .addField("uid", createLong())
                .addField("user", createString())
                .addField("version", createLong())
                .addField("visible", createBoolean())
                .addField("attrs", createUnion()
                    .addUnionChild(createStruct()
                            .addField("lat", createDecimal().withScale(7).withPrecision(9))
                            .addField("lon", createDecimal().withScale(7).withPrecision(10))
                    )
                    .addUnionChild(createList(
                            createStruct()
                                .addField("ref", createLong()))
                    )
                    .addUnionChild(createList(
                            createStruct()
                                .addField("type", createString())
                                .addField("ref", createLong())
                                .addField("role", createString())
                    ))
                );

        Configuration conf = new Configuration();
        // id can be omitted if elements are assumed to be sorted
        // user can be omitted if separate user mappings are kept (to produce a list of uids from a list of usernames)
        conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags,type");
        Writer writer = OrcFile.createWriter(new Path("osm.orc"),
                OrcFile.writerOptions(conf).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector changeset = (LongColumnVector) batch.cols[0];
        LongColumnVector id = (LongColumnVector) batch.cols[1];
        MapColumnVector tags = (MapColumnVector) batch.cols[2];
        TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[3];
        BytesColumnVector type = (BytesColumnVector) batch.cols[4];
        LongColumnVector uid = (LongColumnVector) batch.cols[5];
        BytesColumnVector user = (BytesColumnVector) batch.cols[6];
        LongColumnVector version = (LongColumnVector) batch.cols[7];
        LongColumnVector visible = (LongColumnVector) batch.cols[8];
        UnionColumnVector attrs = (UnionColumnVector) batch.cols[9];

        for (int r = 0; r < 10000; ++r) {
            int row = batch.size++;

            changeset.vector[row] = 1;

            id.vector[row] = r;

            // see OrcMapredRecordWriter for examples of how to do this
            tags.offsets[row] = tags.childCount;
            tags.lengths[row] = 1; // number of key/value pairings
            tags.childCount += tags.lengths[row];
            tags.keys.ensureSize(tags.childCount, tags.offsets[row] != 0);
            tags.values.ensureSize(tags.childCount, tags.offsets[row] != 0);

            // TODO iterate
            ((BytesColumnVector) tags.keys).setVal((int) tags.offsets[row] + 0, "highway".getBytes());
            ((BytesColumnVector) tags.values).setVal((int) tags.offsets[row] + 0, "motorway".getBytes());

            Timestamp now = new Timestamp(System.currentTimeMillis());
            timestamp.time[row] = now.getTime();
            timestamp.nanos[row] = now.getNanos();

            type.setVal(row, "node".getBytes());

            uid.vector[row] = 1234;

            user.setVal(row, "mojodna".getBytes());

            version.vector[row] = 1;
            visible.vector[row] = 1;

            attrs.tags[row] = 0; // node
            StructColumnVector nodeAttrs = (StructColumnVector) attrs.fields[0]; // as a node
            ((DecimalColumnVector) nodeAttrs.fields[0]).set(row, HiveDecimal.create("47.1234567")); // lat
            ((DecimalColumnVector) nodeAttrs.fields[1]).set(row, HiveDecimal.create("-122.1234567")); // lon

            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        writer.close();
    }
}
