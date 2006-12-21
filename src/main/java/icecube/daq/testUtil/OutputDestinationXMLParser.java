/**
 * OutputDestinationXMLParser
 * Date: Sep 16, 2005 10:57:35 AM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.DocumentException;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.net.URL;

/**
 * This is a parser for output destination
 *
 * @author artur
 * @version $Id: OutputDestinationXMLParser.java,v 1.3 2006/07/31 15:38:25 dglo Exp $
 */
public class OutputDestinationXMLParser {

    /**
     * Log object for this class.
     */
    private static final Log log = LogFactory.getLog(OutputDestinationXMLParser.class);

    private OutputDestinationXMLParser() {
    }

    public static OutputDestination[] parseDisposerOutputDestination(String xmlFile) throws DocumentException,
            IOException {

        if (log.isInfoEnabled()) {
            log.info("Parsing xml output destination configuration");
        }

        URL url = InputSourceXMLParser.class.getResource("/" + xmlFile);
        if (url == null) {
            throw new IllegalArgumentException("couldn't find " + xmlFile);
        }

        Document outputDestDocument = null;
        try {
            outputDestDocument = new SAXReader().read(url.openStream());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List outputDests = new ArrayList();

        Element outputDestsElement = outputDestDocument.getRootElement();
        for (Iterator i = outputDestsElement.elementIterator("disposerOutputDest"); i.hasNext();) {
            Element outputDestElement = (Element) i.next();

            Element sourceIdElement = outputDestElement.element("sourceId");
            String sourceIdString = sourceIdElement.getText();
            if (log.isInfoEnabled()) {
                log.info(" SourceId = " + sourceIdString);
            }

            OutputDestination outputDest = new DisposerOutputDestination(Integer.parseInt(sourceIdString));
            outputDests.add(outputDest);
        }
        return (OutputDestination[]) outputDests.toArray(new OutputDestination[outputDests.size()]);
    }

    public static OutputDestination[] parseFileOutput(String xmlFile) throws DocumentException, IOException {

        if (log.isInfoEnabled()) {
            log.info("Parsing xml output destination configuration");
        }

        URL url = InputSourceXMLParser.class.getResource("/" + xmlFile);
        if (url == null) {
            throw new IllegalArgumentException("couldn't find " + xmlFile);
        }

        Document outputDestDocument = null;
        try {
            outputDestDocument = new SAXReader().read(url.openStream());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }


        List outputDests = new ArrayList();

        Element outputDestsElement = outputDestDocument.getRootElement();
        for (Iterator i = outputDestsElement.elementIterator("fileWriterChannel"); i.hasNext();) {
            Element outputDestElement = (Element) i.next();

            Element sourceIdElement = outputDestElement.element("sourceId");
            String sourceIdString = sourceIdElement.getText();
            if (log.isInfoEnabled()) {
                log.info(" SourceId = " + sourceIdString);
            }

            Element fileNameElement = outputDestElement.element("fileName");
            String fileNameString = fileNameElement.getText();
            if (log.isInfoEnabled()) {
                log.info(" fileName = " + fileNameString);
            }

            OutputDestination outputDest = new FileWriterChannel(fileNameString, Integer.parseInt(sourceIdString));
            outputDests.add(outputDest);
        }
        return (OutputDestination[]) outputDests.toArray(new OutputDestination[outputDests.size()]);
    }

    public static void main(String[] args) {
        try {
            OutputDestination[] outputs = OutputDestinationXMLParser.parseDisposerOutputDestination("OutputDestination-config.xml");
            for (int i = 0; i < outputs.length; i++) {
                OutputDestination out = outputs[i];
                System.out.println(out.getSourceID());
            }
            outputs = OutputDestinationXMLParser.parseFileOutput("OutputDestination-config.xml");
            for (int i = 0; i < outputs.length; i++) {
                OutputDestination out = outputs[i];
                System.out.println(out.getSourceID());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
