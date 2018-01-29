package net.mojodna.osm2orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

public class OrcUtils {
    private static final long STRIPE_SIZE = (64L * 1024 * 1024) / 128;

    public static Writer createWriter(String filename, TypeDescription schema) throws IOException {
      Configuration conf = new Configuration();
      // conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "tags");
      OrcFile.WriterOptions writerOptions =
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(STRIPE_SIZE);

      return OrcFile.createWriter(new Path(filename), writerOptions);
    }
}
