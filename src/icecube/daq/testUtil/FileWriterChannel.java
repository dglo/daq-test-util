/**
 * FileWriterChannel
 * Date: Jul 20, 2005 4:15:08 PM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.channels.WritableByteChannel;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileOutputStream;

/**
 * This class will be used to write the output to the destination file
 * @author artur
 * @version $Id: FileWriterChannel.java,v 1.13 2005/11/18 20:07:42 artur Exp $
 */
public class FileWriterChannel implements OutputDestination, Runnable {

    private WritableByteChannel channel;
    private Pipe.SourceChannel sourceChannel;
    private Pipe.SinkChannel sinkChannel;
    private Pipe pipe;
    private int processID = 0;
    private boolean stop = true;
    private boolean running = false;
    private ByteBuffer buf = ByteBuffer.allocate(BufferUtil.DEFAULT_BUFFER_BLEN);
    private String destFileName;

    private Log log = LogFactory.getLog(FileWriterChannel.class);

    // create the channel to read from and write to
    public FileWriterChannel(String destFileName, int processID) throws IOException{
        channel = new FileOutputStream(destFileName).getChannel();
        pipe = Pipe.open();

        sourceChannel = pipe.source();
        sourceChannel.configureBlocking(true);

        sinkChannel = pipe.sink();
        sinkChannel.configureBlocking(false);

        this.processID = processID;
        this.destFileName = destFileName;

        if (log.isInfoEnabled()){
            log.info("created FileWriterChannel ID: " + processID);
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
            log.info("stopped FileWriterChannel ID: " + processID);
        }
    }

    public void startProcessing(){
        if (stop){
            stop = false;
        }
        if (log.isInfoEnabled()){
            log.info("starting FileWriterChannel ID: " + processID);
        }
        running = true;

        Thread thread = new Thread(this);
        thread.setName("FileWriterChannel-" + destFileName);
        thread.start();
    }

    // read from the channel and write to the file
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
                        log.info("FileWriterChannel ID: " + processID + " got a STOP signal");
                    }
                    running = false;
                    return;
                }
                buf.limit(BufferUtil.INT_SIZE);
                channel.write(buf);
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
                buf.position(0);
                channel.write(buf);
                if (log.isDebugEnabled()){
                    log.debug("write ByteBuffer to file from Channel ID: " + processID + " length: " + recLength);
                }
            } catch(IOException ioe){
                throw new RuntimeException(ioe.getMessage());
            }
        }
    }
}

