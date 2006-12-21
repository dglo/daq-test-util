/**
 * InputSourceManager
 * Date: Sep 14, 2005 9:18:26 AM
 * <p/>
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import java.io.IOException;

/**
 * This interface manages one or more input sources
 *
 * @author artur
 * @version $Id: InputSourceManager.java,v 1.1 2005/09/17 01:32:45 artur Exp $
 */
public interface InputSourceManager {

    /**
     * get the number of InputSources managed by this interface
     * @return an integer value
     */
    public int getInputSourcesCount();

    /**
     * start processing of all InputSources
     * @throws IOException
     */
    public void startProcessing() throws IOException;

    /**
     * stop processing of all InputSources
     * @throws IOException
     */
    public void stopProcessing() throws IOException;

    /**
     *
     * @return an array of InputSource(s)
     */
    public InputSource[] getInputSources();

    /**
     * check to see if the InputSources are running
     * @return a boolean value
     */
    public boolean isRunning();
}
