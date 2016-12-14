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

    private ChangesetCallback changesetCallback;
    private Changeset changeset;
    private TagElementProcessor tagElementProcessor;


    public ChangesetElementProcessor(BaseElementProcessor parentProcessor, ChangesetCallback changesetCallback) {
        super(parentProcessor, true);
        this.changesetCallback = changesetCallback;
        tagElementProcessor = new TagElementProcessor(this, this);
    }

    @Override
    public void begin(Attributes attributes) {
        long id;
        String created_atStr;
        String closed_atStr;
        TimestampContainer created_at = null;
        TimestampContainer closed_at = null;
        boolean open;
        long num_changes;
        String user;
        long uid;
        Double min_lat;
        Double max_lat;
        Double min_lon;
        Double max_lon;
        long comments_count;

        id = Long.parseLong(attributes.getValue(Changeset.ID));

        // Created / closed at timestamps are not guaranteed.
        created_atStr = attributes.getValue(Changeset.CREATED_AT);
        closed_atStr = attributes.getValue(Changeset.CLOSED_AT);
        if (created_atStr != null) {
            created_at = createTimestampContainer(created_atStr);
        }
        if (closed_atStr != null) {
            closed_at = createTimestampContainer(closed_atStr);
        }

        open = attributes.getValue(Changeset.OPEN).equals("true");
        num_changes = Long.parseLong(attributes.getValue(Changeset.NUM_CHANGES));
        user = attributes.getValue(Changeset.USER);
        uid = Long.parseLong(attributes.getValue(Changeset.UID));

        // We also might not have min/max lat/lng. For example, open changesets...
        try {
            min_lat = Double.parseDouble(attributes.getValue(Changeset.MIN_LAT));
        } catch (Exception e) {
            min_lat = null;
        }
        try {
            max_lat = Double.parseDouble(attributes.getValue(Changeset.MAX_LAT));
        } catch (Exception e) {
            max_lat = null;
        }
        try {
            min_lon = Double.parseDouble(attributes.getValue(Changeset.MIN_LON));
        } catch (Exception e) {
            min_lon = null;
        }
        try {
            max_lon = Double.parseDouble(attributes.getValue(Changeset.MAX_LON));
        } catch (Exception e) {
            max_lon = null;
        }

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
        changesetCallback.call(changeset);
    }

    @Override
    public void processTag(Tag tag) {
        changeset.getTags().put(tag.getKey(), tag.getValue());
    }
}
