package net.mojodna.osm2orc.standalone.parser;


import net.mojodna.osm2orc.standalone.model.Changeset;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.xml.common.ElementProcessor;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.List;
import java.util.logging.Logger;

/**
 * See org.openstreetmap.osmosis.xml.v0_6.impl.OsmHandler.java
 */
public class ChangesetXmlHandler extends DefaultHandler {
    private static final Logger LOG = Logger.getLogger(ChangesetXmlHandler.class.getName());
    private static final String ELEMENT_NAME_OSM = "osm";

    private ElementProcessor changesetOsmElementProcessor;
    private ElementProcessor elementProcessor;
    private Locator documentLocator;

    public ChangesetXmlHandler(ChangesetCallback changesetCallback) {
        this.changesetOsmElementProcessor = new ChangesetOsmElementProcessor(changesetCallback);
    }

    /**
     * Begins processing of a new element.
     *
     * @param uri
     *            The uri.
     * @param localName
     *            The localName.
     * @param qName
     *            The qName.
     * @param attributes
     *            The attributes.
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        // Get the appropriate element processor for the element.
        if (elementProcessor != null) {
            // We already have an active element processor, therefore use the
            // active element processor to retrieve the appropriate child
            // element processor.
            elementProcessor = elementProcessor.getChild(uri, localName, qName);
        } else if (ELEMENT_NAME_OSM.equals(qName)) {
            // There is no active element processor which means we have
            // encountered the root osm element.
            elementProcessor = changesetOsmElementProcessor;
        } else {
            // There is no active element processor which means that this is a
            // root element. The root element in this case does not match the
            // expected name.
            throw new OsmosisRuntimeException("This does not appear to be an OSM Changeset XML file.");
        }

        // Initialise the element processor with the attributes of the new element.
        elementProcessor.begin(attributes);
    }


    /**
     * Ends processing of the current element.
     *
     * @param uri
     *            The uri.
     * @param localName
     *            The localName.
     * @param qName
     *            The qName.
     */
    @Override
    public void endElement(String uri, String localName, String qName) {
        // Tell the currently active element processor to complete its processing.
        elementProcessor.end();

        // Set the active element processor to the parent of the existing processor.
        elementProcessor = elementProcessor.getParent();
    }


    /**
     * Sets the document locator which is used to report the position in the
     * file when errors occur.
     *
     * @param documentLocator
     *            The document locator.
     */
    @Override
    public void setDocumentLocator(Locator documentLocator) {
        this.documentLocator = documentLocator;
    }


    /**
     * Called by the SAX parser when an error occurs. Used by this class to
     * report the current position in the file.
     *
     * @param e
     *            The exception that occurred.
     * @throws SAXException
     *             if the error reporting throws an exception.
     */
    @Override
    public void error(SAXParseException e) throws SAXException {
        LOG.severe(
                "Unable to parse changeset xml file.  publicId=(" + documentLocator.getPublicId()
                        + "), systemId=(" + documentLocator.getSystemId()
                        + "), lineNumber=" + documentLocator.getLineNumber()
                        + ", columnNumber=" + documentLocator.getColumnNumber() + ".");

        super.error(e);
    }
}
