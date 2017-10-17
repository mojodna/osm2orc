package net.mojodna.osm2orc;

import net.mojodna.osm2orc.standalone.OsmChangesetXml2Orc;
import net.mojodna.osm2orc.standalone.OsmPbf2Orc;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class Osm2Orc {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: osm2orc [--changesets] <input> <output>");
            System.exit(1);
        }

        if (args[0].equals("--changesets")) {
            final InputStream inputStream;

            if (args[1].equals("-")) {
                inputStream = System.in;
            } else {
                inputStream = new FileInputStream(args[1]);
            }

            new OsmChangesetXml2Orc(inputStream, args[2]).convert();
            System.exit(0);
        }

        final InputStream inputStream;

        if (args[0].equals("-")) {
            inputStream = System.in;
        } else {
            inputStream = new FileInputStream(args[0]);
        }

        OsmPbf2Orc.convert(inputStream, args[1]);
        System.exit(0);
    }
}
