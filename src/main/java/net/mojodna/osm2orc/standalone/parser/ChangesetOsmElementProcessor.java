package net.mojodna.osm2orc.standalone.parser;


import net.mojodna.osm2orc.standalone.model.Changeset;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.BaseElementProcessor;
import org.openstreetmap.osmosis.xml.common.ElementProcessor;
import org.openstreetmap.osmosis.xml.v0_6.impl.*;
import org.xml.sax.Attributes;

import java.util.List;
import java.util.logging.Logger;

public class ChangesetOsmElementProcessor extends SourceElementProcessor {

    private final List<Changeset> changesets;


    private static final Logger LOG = Logger.getLogger(OsmElementProcessor.class.getName());

    private static final String ELEMENT_NAME_CHANGESET = "changeset";

    private ChangesetElementProcessor changesetElementProcessor;

    public ChangesetOsmElementProcessor(List<Changeset> changesets) {
        // parentProcessor, sink, enableDateParsing
        super(null, null, true);

        this.changesets = changesets;
        changesetElementProcessor = new ChangesetElementProcessor(this, changesets);
    }


    /**
     * {@inheritDoc}
     */
    public void begin(Attributes attributes) {
        // Meh, we don't care about the osm element attributes at this point...
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
        if (ELEMENT_NAME_CHANGESET.equals(qName)) {
            return changesetElementProcessor;
        }

        return super.getChild(uri, localName, qName);
    }


    /**
     * {@inheritDoc}
     */
    public void end() {
        // This class produces no data and therefore doesn't need to do anything
        // when the end of the element is reached.
    }
}
