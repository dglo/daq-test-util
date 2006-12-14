/**
 * DisposerOutputDestination
 * Date: Sep 13, 2005 3:04:29 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author artur
 * @version $Id: DisposerOutputDestination.java,v 1.6 2005/10/26 23:05:22 artur Exp $
 */
public class DisposerOutputDestination implements OutputDestination, Runnable {

    private Pipe.SourceChannel sourceChannel;
    private Pipe.SinkChannel sinkChannel;
    private Pipe pipe;
    private int processID = 0;
    private boolean stop = true;
    private ByteBuffer buf = ByteBuffer.allocate(BufferUtil.DEFAULT_BUFFER_BLEN);
    private boolean running = false;

    private Log log = LogFactory.getLog(DisposerOutputDestination.class);

    // create the channel to read from and write to
    public DisposerOutputDestination(int processID) throws IOException{

        pipe = Pipe.open();

        sourceChannel = pipe.source();
        sourceChannel.configureBlocking(true);

        sinkChannel = pipe.sink();
        sinkChannel.configureBlocking(false);

        this.processID = processID;

        if (log.isInfoEnabled()){
            log.info("created DisposerOutputDestination ID: " + processID);
        }
    }

    public SelectableChannel getSinkChannel(){
        return sinkChannel;
    }

    public int getSourceID(){
        return processID;
    }

    public boolean isRunning() {
        return running;
    }

    public void stopProcessing(){
        if (!stop){
            stop = true;
        }
        if (log.isInfoEnabled()){
            log.info("stopped DisposerOutputDestination ID: " + processID);
        }
    }

    public void startProcessing(){
        if (stop){
            stop = false;
        }
        if (log.isInfoEnabled()){
            log.info("starting DisposerOutputDestination ID: " + processID);
        }
        running = true;
        new Thread(this).start();
    }

    // read from the channel and recycle the ByteBuffer
    public void run(){

        while(!stop){
            try {
                buf.limit(BufferUtil.INT_SIZE);
                buf.position(0);
                while (buf.hasRemaining()) {
                    sourceChannel.read(buf);
                }
                buf.position(0);
                int recLength = buf.getInt(0);
                if (recLength == BufferUtil.INT_SIZE){
                    stop = true;
                    if (log.isInfoEnabled()){
                        log.info("DisposerOutputDestination ID: " + processID + " got a STOP signal");
                    }
                    running = false;
                    return;
                }
                buf.clear();
                if (recLength > BufferUtil.DEFAULT_BUFFER_BLEN){
                    buf = ByteBuffer.allocate(recLength);
                    System.out.println("Creating a new ByteBuffer of length: " + recLength);
                }
                buf.limit(recLength - BufferUtil.INT_SIZE);
                buf.position(0);
                while (buf.hasRemaining()) {
                    sourceChannel.read(buf);
                }
                buf.clear();
                if (log.isDebugEnabled()){
                    log.debug("dispose ByteBuffer from Channel ID: " + processID + " length: " + recLength);
                }
            } catch(IOException ioe){

                throw new RuntimeException(ioe.getMessage());
            }
        }
    }
}
