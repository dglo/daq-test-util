/**
 * FileReaderChannel
 * Date: Jun 24, 2005 11:07:55 AM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import icecube.daq.payload.PayloadReader;

import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.io.EOFException;
import java.util.Observable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileReaderChannel {

    private Pipe.SinkChannel sinkChannel;
    private Pipe.SourceChannel sourceChannel;
    private Pipe pipe;
    private int processID = 0;
    private boolean done = false;
    private PayloadReader payloadReader;
    private final int DONE_SIGNAL = 4;
    private Log log = LogFactory.getLog(FileReaderChannel.class);

    public FileReaderChannel(String sourceFileName, int processID) throws IOException {

        if (!(new File(sourceFileName).exists())){
            throw new IllegalArgumentException(sourceFileName + " does not exist.");
        }
        payloadReader = new PayloadReader(sourceFileName);
        payloadReader.open();
        pipe = Pipe.open();
        sinkChannel = pipe.sink();
        sourceChannel = pipe.source();
        sinkChannel.configureBlocking(true);
        sourceChannel.configureBlocking(false);

        this.processID = processID;
    }

    public SelectableChannel getSourceChannel(){
        return sourceChannel;
    }

    // read from the file channel to a ByteBuffer and write the byte buffer to the SinkChannel
    void read(ByteBuffer buf) {
        if (done){
            return;
        }
        buf.clear();
        int header = 0;
        try {
            payloadReader.readNextPayload(buf);
            buf.clear();
            header = buf.getInt(0);
            buf.limit(header);
        } catch (EOFException eofEx){
            buf.clear();
            buf.limit(DONE_SIGNAL);
            buf.putInt(0, DONE_SIGNAL);
            done = true;
            if (log.isInfoEnabled()){
                log.info("END OF FILE from Channel " + processID);
            }
            try {
                payloadReader.close();
            } catch(IOException e){
                throw new RuntimeException(e);
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        try {
            //buf.clear();
            int nWrite = sinkChannel.write(buf);
            if (nWrite != header) {
                log.error("Payload is " + header + " bytes, but only wrote " + nWrite + " bytes!");
            }
            //buf.clear();
        } catch(Exception e){
            throw new RuntimeException(e);
        }
        //notifyChanges(new MsgByteBuffer(buf, processID));
    }

    // make this method package visible
    boolean isDone(){
        return done;
    }

    void stop() throws IOException{
        payloadReader.close();
        sinkChannel.close();
    }
}
