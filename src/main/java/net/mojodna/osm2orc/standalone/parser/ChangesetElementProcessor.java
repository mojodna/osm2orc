package net.mojodna.osm2orc.standalone.parser;


import net.mojodna.osm2orc.standalone.model.Changeset;
import org.openstreetmap.osmosis.core.domain.common.TimestampContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.xml.common.BaseElementProcessor;
import org.openstreetmap.osmosis.xml.common.ElementProcessor;
import org.openstreetmap.osmosis.xml.v0_6.impl.TagElementProcessor;
import org.openstreetmap.osmosis.xml.v0_6.impl.TagListener;
import org.xml.sax.Attributes;

import java.util.List;

public class ChangesetElementProcessor extends BaseElementProcessor implements TagListener {

    private List<Changeset> changesets;
    private Changeset changeset;
    private TagElementProcessor tagElementProcessor;


    public ChangesetElementProcessor(BaseElementProcessor parentProcessor, List<Changeset> changesets) {
        super(parentProcessor, true);
        this.changesets = changesets;
        tagElementProcessor = new TagElementProcessor(this, this);
    }

    @Override
    public void begin(Attributes attributes) {
        long id;
        TimestampContainer created_at;
        TimestampContainer closed_at;
        boolean open;
        long num_changes;
        String user;
        long uid;
        double min_lat;
        double max_lat;
        double min_lon;
        double max_lon;
        long comments_count;

        id = Long.parseLong(attributes.getValue(Changeset.ID));
        created_at = createTimestampContainer(attributes.getValue(Changeset.CREATED_AT));
        closed_at = createTimestampContainer(attributes.getValue(Changeset.CLOSED_AT));
        open = attributes.getValue(Changeset.OPEN).equals("true");
        num_changes = Long.parseLong(attributes.getValue(Changeset.NUM_CHANGES));
        user = attributes.getValue(Changeset.USER);
        uid = Long.parseLong(attributes.getValue(Changeset.UID));
        min_lat = Double.parseDouble(attributes.getValue(Changeset.MIN_LAT));
        max_lat = Double.parseDouble(attributes.getValue(Changeset.MAX_LAT));
        min_lon = Double.parseDouble(attributes.getValue(Changeset.MIN_LON));
        max_lon = Double.parseDouble(attributes.getValue(Changeset.MAX_LON));
        comments_count = Long.parseLong(attributes.getValue(Changeset.COMMENTS_COUNT));

        changeset = new Changeset(id, created_at, closed_at, open, num_changes, user, uid,
                                    min_lat, max_lat, min_lon, max_lon, comments_count);
    }

    /**
     * Retrieves the appropriate child element processor for the newly
     * encountered nested element.
     *
     * @param uri
     *            The element uri.
     * @param localName
     *            The element localName.
     * @param qName
     *            The element qName.
     * @return The appropriate element processor for the nested element.
     */
    @Override
    public ElementProcessor getChild(String uri, String localName, String qName) {
        if (Changeset.TAG.equals(qName)) {
            return tagElementProcessor;
        }

        return super.getChild(uri, localName, qName);
    }

    @Override
    public void end() {
        changesets.add(changeset);
    }

    @Override
    public void processTag(Tag tag) {
        changeset.getTags().put(tag.getKey(), tag.getValue());
    }
}
