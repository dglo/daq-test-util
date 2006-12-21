/**
 * InputSourceGeneratorMng
 * Date: Sep 14, 2005 9:29:21 AM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import icecube.daq.sim.GenericHit;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author artur
 * @version $Id: InputSourceGeneratorMng.java,v 1.3 2005/10/20 00:27:31 artur Exp $
 */
public class InputSourceGeneratorMng implements InputSourceManager {

    protected InputSource[] inputSources;
    private long seconds = 1;

    public InputSourceGeneratorMng(InputSource[] inputSources) {
        if (inputSources == null || inputSources.length == 0) {
            throw new IllegalArgumentException("Error: InputSources is either null or 0 length");
        }
        this.inputSources = inputSources;
    }

    /**
     * get the number of InputSources managed by this interface
     *
     * @return an integer value
     */
    public int getInputSourcesCount() {
        return inputSources.length;
    }

    /**
     * start processing of all InputSources
     *
     * @throws java.io.IOException
     */
    public void startProcessing() throws IOException {
        if (isRunning()) {
            return;
        }
        for (int i = 0; i < inputSources.length; i++) {
            inputSources[i].startProcessing();
        }
    }

    public void startSingleThreadProcessing() throws IOException {
        long currentTime = System.currentTimeMillis();
        long duration = seconds * 1000;
        long totalTime = currentTime + duration;
        while (totalTime > currentTime) {
            for (int i = 0; i < inputSources.length; i++) {
                ((InputSourceGenerator) inputSources[i]).generatePayload();
            }
            currentTime = System.currentTimeMillis();
        }
        for (int i = 0; i < inputSources.length; i++) {
            ((InputSourceGenerator) inputSources[i]).sendStopSignal();
        }
    }


    /**
     * stop processing of all InputSources
     *
     * @throws IOException
     */
    public void stopProcessing() throws IOException {
        if (!isRunning()) {
            return;
        }
        for (int i = 0; i < inputSources.length; i++) {
            inputSources[i].stopProcessing();
        }
    }

    /**
     * @return an array of InputSource(s)
     */
    public InputSource[] getInputSources() {
        return inputSources;
    }

    /**
     * check to see if the InputSources are running
     *
     * @return a boolean value
     */
    public boolean isRunning() {
        boolean isRunning = false;
        for (int i = 0; i < inputSources.length; i++) {
            if (inputSources[i].isRunning()) {
                isRunning = true;
                break;
            }
        }
        return isRunning;
    }
}
