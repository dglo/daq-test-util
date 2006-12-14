/**
 * FileInputSourceMng
 * Date: Sep 14, 2005 10:12:21 AM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author artur
 * @version $Id: FileInputSourceMng.java,v 1.9 2006/01/17 09:01:35 artur Exp $
 */
public class FileInputSourceMng extends InputSourceGeneratorMng {

    private List sources;

    private Log log = LogFactory.getLog(FileInputSourceMng.class);

   // private HashMap map;

    public FileInputSourceMng(InputSource[] inputSources) {
        super(inputSources);
        sources = new ArrayList();
        for (int i = 0; i < inputSources.length; i++){
            InputSourceInfo inputSourceInfo = new InputSourceInfo(inputSources[i]);
            sources.add(inputSourceInfo);
        }
    }

    /**
     * start processing of all InputSources
     *
     * @throws java.io.IOException
     */
    public void startProcessing() throws IOException {
        Thread thread = new Thread(new FileInputThread());
        StringBuffer buf;
        if (sources.size() > 1) {
            buf = new StringBuffer("FileInputSources[");
        } else {
            buf = new StringBuffer("FileInputSource ");
        }
        for (int i = 0; i < sources.size(); i++) {
            if (i > 0) {
                buf.append(',');
            }
            InputSourceInfo inputSourceInfo = (InputSourceInfo)sources.get(i);
            buf.append(inputSourceInfo.getInputSource().getSourceID());
        }
        if (sources.size() > 1) {
            buf.append(']');
        }
        thread.setName(buf.toString());
        thread.start();
    }

    class FileInputThread implements Runnable {
        public void run() {
            int inputSourceIndex = 0;
            while (sources.size() > 0) {
                try {
                    InputSourceInfo inputSourceInfo = (InputSourceInfo)sources.get(inputSourceIndex);
                    InputSource inputSource = inputSourceInfo.getInputSource();
                    if (log.isDebugEnabled()) {
                        log.debug("Reading from FileInputSource ID = " + inputSource.getSourceID());
                    }

                    if (!inputSource.isRunning()) {
                        sources.remove(inputSourceInfo);
                    } else {
                        inputSource.startProcessing();
                        if (inputSource.getNumDoms() > 0){
                            if (!inputSourceInfo.hasInitialPacket()){
                                inputSourceInfo.setInitialPacket(true);
                            } else {
                                if (inputSourceInfo.getDomCounter() == inputSource.getNumDoms()){
                                    long sleep = (long)inputSource.getRate();
                                    Thread.sleep(sleep);
                                    inputSourceInfo.resetDomCounter();
                                } else {
                                    inputSourceInfo.increaseDomCounter();
                                }
                            }
                        }
                    }

                    if (inputSourceIndex < sources.size() - 1) {
                        ++inputSourceIndex;
                    } else {
                        inputSourceIndex = 0;
                    }
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        log.error("Problem in FileInputSource: ", e);
                    }
                }
            }
            if (log.isInfoEnabled()) {
                log.info("Completed reading from files. Sending stop signal...");
            }
        }
    }

    class InputSourceInfo {

        private InputSource inputSource;
        private int domCounter = 0;
        private boolean initialPacket = false;

        public InputSourceInfo(InputSource inputSource){
            this.inputSource = inputSource;
        }

        public InputSource getInputSource(){
            return inputSource;
        }

        public int getDomCounter(){
            return domCounter;
        }

        public void increaseDomCounter(){
            ++domCounter;
        }

        public void resetDomCounter(){
            domCounter = 0;
        }

        public boolean hasInitialPacket(){
            return initialPacket;
        }

        public void setInitialPacket(boolean initialPacket){
            this.initialPacket = initialPacket;
        }
    }
}
