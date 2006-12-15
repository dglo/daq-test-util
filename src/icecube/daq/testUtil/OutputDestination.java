package icecube.daq.testUtil;

import java.nio.channels.SelectableChannel;

/**
 * OutputDestination
 * Date: Sep 13, 2005 2:52:48 PM
 * <p/>
 * (c) 2005 IceCube Collaboration
 */
public interface OutputDestination {

    /**
     * start processing for this object
     */
    public void startProcessing();

    /**
     * stop processing for this object
     */
    public void stopProcessing();

    /**
     * check to see if the output process is running
     * @return a boolean value
     */
    public boolean isRunning();

    /**
     * get the sink channel which is also a SelectableChannel
     * @return a Pipe.SinkChannel object
     */
    public SelectableChannel getSinkChannel();

    /**
     * get the source id for this output destination
     * @return sourceID
     */
    public int getSourceID();
}
