/**
 * SingleFileInputSourceMng
 * Date: Oct 25, 2005 1:02:13 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * This class will run a single input channel from a file input source
 *
 * @author artur
 * @version $Id: SingleFileInputSourceMng.java,v 1.1 2005/10/26 01:32:00 artur Exp $
 */
public class SingleFileInputSourceMng extends InputSourceGeneratorMng {

    private Log log = LogFactory.getLog(SingleFileInputSourceMng.class);

    private InputSource[] inputSources;

    public SingleFileInputSourceMng(InputSource[] inputSources) {
        super(inputSources);
        this.inputSources = inputSources;
    }

    // feeds a singel input channel from a list of payloads
    public void startProcessing() throws IOException {

        InputSource inputSource = inputSources[0];

        inputSource.startProcessing();

        if (log.isInfoEnabled()) {
            log.info("Completed reading from files. Sending stop signal...");
        }
    }
}
