/**
 * InputSourceXMLParser
 * Date: Sep 14, 2005 3:13:16 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.io.SAXReader;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.DocumentException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.io.File;
import java.net.URL;

/**
 * This class can be used to configure one or more InputSourceGenerator from an xml file
 *
 * @author artur
 * @version $Id: InputSourceXMLParser.java,v 1.9 2006/01/17 02:29:36 artur Exp $
 */
public class InputSourceXMLParser {

    /**
     * Log object for this class.
     */
    private static final Log log = LogFactory.getLog(InputSourceXMLParser.class);

    private InputSourceXMLParser() {}

    public static InputSource[] parseGenerator(String xmlFile) throws DocumentException {

        if (log.isInfoEnabled()) {
            log.info("Parsing xml trigger configuration");
        }

        URL url = InputSourceXMLParser.class.getResource("/" + xmlFile);
        if (url == null) {
            throw new IllegalArgumentException("couldn't find " + xmlFile);
        }

        Document inputSourceDocument = null;
        try {
            inputSourceDocument = new SAXReader().read(url.openStream());
        } catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
        List inputSources = new ArrayList();

        Element inputSourcesElement = inputSourceDocument.getRootElement();
        for (Iterator i = inputSourcesElement.elementIterator("inputSourceGenerator"); i.hasNext();) {
            Element inputSourceElement = (Element) i.next();

            InputSource inputSource = null;
            Element seedElement = inputSourceElement.element("seed");
            if (seedElement != null){
                inputSource = new InputSourceGenerator(new Long(seedElement.getText()));
            } else {
                inputSource = new InputSourceGenerator();
            }

            // start with fixed length elements
            Element rateElement = inputSourceElement.element("rate");
            String rate = rateElement.getText();
            inputSource.setRate(Double.parseDouble(rate));

            Element numDomsElement = inputSourceElement.element("numDoms");
            String numDoms = numDomsElement.getText();
            inputSource.setNumDoms(Integer.parseInt(numDoms));

            Element sourceIdElement = inputSourceElement.element("sourceId");
            String sourceIdString = sourceIdElement.getText();
            inputSource.setSourceID(Integer.parseInt(sourceIdString));

            Element triggerModeElement = inputSourceElement.element("triggerMode");
            String triggerMode = triggerModeElement.getText();
            inputSource.setTriggerMode(Integer.parseInt(triggerMode));

            Element payloadTypeElement = inputSourceElement.element("payloadType");
            String payloadType = payloadTypeElement.getText();
            inputSource.setPayloadType(payloadType);

            Element procDurationElement = inputSourceElement.element("procDuration");
            String duration = procDurationElement.getText();
            inputSource.setProcessDuration(Integer.parseInt(duration));

            Element totalHitsElement = inputSourceElement.element("totalHits");
            String totalHits = totalHitsElement.getText();
            inputSource.setNumOfHits(Long.parseLong(totalHits));

            inputSources.add(inputSource);
        }
        return (InputSource[])inputSources.toArray(new InputSource[inputSources.size()]);
    }

    public static InputSource[] parseEventGenerator(String xmlFile) throws Exception {

            if (log.isInfoEnabled()) {
                log.info("Parsing eventInputGenerator");
            }

            URL url = InputSourceXMLParser.class.getResource("/" + xmlFile);
            if (url == null) {
                throw new IllegalArgumentException("couldn't find " + xmlFile);
            }

            Document inputSourceDocument = null;
            try {
                inputSourceDocument = new SAXReader().read(url.openStream());
            } catch(IOException ioe){
                throw new RuntimeException(ioe);
            }
            List inputSources = new ArrayList();

            Element inputSourcesElement = inputSourceDocument.getRootElement();
            for (Iterator i = inputSourcesElement.elementIterator("inputSourceGenerator"); i.hasNext();) {
                Element inputSourceElement = (Element) i.next();
                Element seedElement = inputSourceElement.element("randSeed");
                long seed = 0;
                if (seedElement != null){
                    seed = Long.parseLong(seedElement.getText());
                }

                Element numGenEventElement = inputSourceElement.element("numEventGen");
                if (numGenEventElement == null){
                    throw new RuntimeException("numEventGen element must not be null");
                }
                int numGenerated = Integer.parseInt(numGenEventElement.getText());

                int maxHitsPerTrigger = 1;
                EventInputSourceGenerator evtGen = null;
                if (seed == 0) {
                    evtGen = new EventInputSourceGenerator(maxHitsPerTrigger);
                } else {
                    evtGen = new EventInputSourceGenerator(seed, maxHitsPerTrigger);
                }

                evtGen.setNumOfHits(numGenerated);
                InputSource gtInputSource = evtGen.getTriggerRequestSource();
                inputSources.add(gtInputSource);

                InputSource[] spInputSources = evtGen.getHitSources();
                for (int s = 0; s < spInputSources.length; s++) {
                    inputSources.add(spInputSources[s]);
                }
            }
            return (InputSource[])inputSources.toArray(new InputSource[inputSources.size()]);
        }


    public static InputSource[]  parseFileInput(String xmlFile) throws DocumentException, IOException{

        if (log.isInfoEnabled()) {
            log.info("Parsing xml trigger configuration");
        }

        URL url = InputSourceXMLParser.class.getResource("/" + xmlFile);
        if (url == null) {
            throw new IllegalArgumentException("couldn't find " + xmlFile);
        }

        Document inputSourceDocument = null;
        try {
            inputSourceDocument = new SAXReader().read(url.openStream());
        } catch(IOException ioe){
            throw new RuntimeException(ioe);
        }

        List inputSources = new ArrayList();

        // iterate over triggers
        Element inputSourcesElement = inputSourceDocument.getRootElement();
        for (Iterator i = inputSourcesElement.elementIterator("inputSourceFile"); i.hasNext();) {
            Element inputSourceElement = (Element) i.next();
            Element sourceIdElement = inputSourceElement.element("sourceId");
            String sourceIdString = sourceIdElement.getText();
            if (log.isInfoEnabled()) {
                log.info(" SourceId = " + sourceIdString);
            }

            Element fileNameElement = inputSourceElement.element("fileName");
            String fileName = fileNameElement.getText();
            if (!(new File(fileName).exists())){
                log.error(fileName + " does not exists!");
                return null;
            }
            if (log.isInfoEnabled()) {
                log.info(" File Name = " + fileName);
            }

            InputSource inputSource = new FileInputSource(fileName, Integer.parseInt(sourceIdString));

            Element rateElement = inputSourceElement.element("rate");
            if (rateElement != null){
                inputSource.setRate(Double.parseDouble(rateElement.getText()));
            }
            Element numDomsElement = inputSourceElement.element("numDoms");
            if (numDomsElement != null){
                inputSource.setNumDoms(Integer.parseInt(numDomsElement.getText()));
            }
            inputSources.add(inputSource);
        }
        return (InputSource[])inputSources.toArray(new InputSource[inputSources.size()]);
    }

    public static void main(String[] args){
        try {
            InputSource[] inputSources = InputSourceXMLParser.parseGenerator("InputSourceGenerator-config.xml");
            for (int i = 0; i < inputSources.length; i++){
                InputSource is = inputSources[i];
                System.out.println(is.getNumDoms() +  " - " + is.getPayloadType() + " - " + is.getProcessDuration() + " - " + is.getRate() +  " - " + is.getSourceID() + " - " + is.getTriggerMode());
            }
            inputSources = InputSourceXMLParser.parseFileInput("InputSourceGenerator-config.xml");
            for (int i = 0; i < inputSources.length; i++){
                InputSource is = inputSources[i];
                System.out.println(is.getSourceID());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
