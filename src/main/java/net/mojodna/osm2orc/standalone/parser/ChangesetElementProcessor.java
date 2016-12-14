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
        String createdAtStr;
        String closedAtStr;
        TimestampContainer createdAt = null;
        TimestampContainer closedAt = null;
        boolean open;
        long numChanges;
        String user;
        Long uid;
        String minLat;
        String maxLat;
        String minLon;
        String maxLon;
        long commentsCount;

        id = Long.parseLong(attributes.getValue(Changeset.ID));

        // Created / closed at timestamps are not guaranteed.
        createdAtStr = attributes.getValue(Changeset.CREATED_AT);
        closedAtStr = attributes.getValue(Changeset.CLOSED_AT);
        if (createdAtStr != null) {
            createdAt = createTimestampContainer(createdAtStr);
        }
        if (closedAtStr != null) {
            closedAt = createTimestampContainer(closedAtStr);
        }

        open = attributes.getValue(Changeset.OPEN).equals("true");
        numChanges = Long.parseLong(attributes.getValue(Changeset.NUM_CHANGES));
        user = attributes.getValue(Changeset.USER);

        try {
            uid = Long.parseLong(attributes.getValue(Changeset.UID));
        } catch (Exception e) {
            uid = null;
        }

        minLat = attributes.getValue(Changeset.MIN_LAT);
        maxLat = attributes.getValue(Changeset.MAX_LAT);
        minLon = attributes.getValue(Changeset.MIN_LON);
        maxLon = attributes.getValue(Changeset.MAX_LON);

        commentsCount = Long.parseLong(attributes.getValue(Changeset.COMMENTS_COUNT));

        changeset = new Changeset(id, createdAt, closedAt, open, numChanges, user, uid,
                                    minLat, maxLat, minLon, maxLon, commentsCount);
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
