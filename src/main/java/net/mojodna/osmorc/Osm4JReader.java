package net.mojodna.osmorc;

import de.topobyte.osm4j.core.access.OsmIterator;
import de.topobyte.osm4j.core.model.iface.EntityContainer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import de.topobyte.osm4j.pbf.seq.PbfIterator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import static de.topobyte.osm4j.core.model.iface.EntityType.Node;
import static de.topobyte.osm4j.core.model.iface.EntityType.Relation;
import static de.topobyte.osm4j.core.model.iface.EntityType.Way;

public class Osm4JReader {
    public static void main(String[] args) throws Exception {
        InputStream input = new FileInputStream("seattle_washington.osm.pbf");

        OsmIterator iterator = new PbfIterator(input, true);

        for (EntityContainer container : iterator) {
            OsmEntity entity = container.getEntity();

            Map<String, String> tags = OsmModelUtil.getTagsAsMap(entity);

            switch (container.getType()) {
            default:
            case Node:
                break;
            case Way:
                break;
            case Relation:
                break;
            }
        }
    }
}
