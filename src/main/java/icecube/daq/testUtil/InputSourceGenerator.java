/**
 * InputSourceGenerator
 * Date: Sep 12, 2005 1:11:39 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.channels.SelectableChannel;
import java.nio.channels.Pipe;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Random;

import icecube.daq.sim.*;

/**
 * @author artur
 * @version $Id: InputSourceGenerator.java,v 1.15 2005/11/10 05:48:49 artur Exp $
 */
public class InputSourceGenerator implements InputSource {

    private Pipe.SinkChannel sinkChannel;
    private Pipe.SourceChannel sourceChannel;
    private boolean isRunning = false;
    private IGenerator generator;
    private double rate = 0.0000001;   // 1 kHz in units of 1/10 ns
    private int numDoms = 60;
    private int sourceID = 4000;
    private int triggerMode = 2;
    private long numHits = 0;
    private int minutes = 1;
    private String payloadType;
    private Long seed = null;
    private long totalHits = 100000;
    private Random domGenerator = new Random();

    private final String HIT_DATA_PAYLOAD = "HitDataPayload";
    private final String HIT_PAYLOAD = "HitPayload";

    private Log log = LogFactory.getLog(InputSourceGenerator.class);

    public InputSourceGenerator() {
        generator = new icecube.daq.sim.HitGenerator();
        payloadType = HIT_PAYLOAD;
        try {
            Pipe pipe = Pipe.open();
            sinkChannel = pipe.sink();
            sourceChannel = pipe.source();
            sinkChannel.configureBlocking(true);
            sourceChannel.configureBlocking(false);
            if (log.isInfoEnabled()) {
                log.info("creating InputSourceGenerator");
            }
        } catch (IOException ioe) {
            if (log.isErrorEnabled()) {
                log.error("Unknown Payload type");
            }
            throw new RuntimeException("couldn't create a Pipe object");
        }
    }

    public InputSourceGenerator(Long seed) {
        this();
        this.seed = seed;
    }

    public void setRate(double rate) {
        if (isRunning) {
            if (log.isWarnEnabled()) {
                log.warn("cannot change the rate while InputSourceGenerator is running");
            }
            return;
        }
        this.rate = rate;
        if (log.isInfoEnabled()) {
            log.info("Rate = " + rate);
        }
    }

    public double getRate() {
        return rate;
    }

    public void setNumDoms(int numDoms) {
        if (isRunning) {
            if (log.isWarnEnabled()) {
                log.warn("cannot change num of doms while InputSourceGenerator is running");
            }
            return;
        }
        this.numDoms = numDoms;
        if (log.isInfoEnabled()) {
            log.info("Num Doms = " + numDoms);
        }
    }

    public int getNumDoms() {
        return numDoms;
    }

    public void setSourceID(int sourceID) {
        if (isRunning) {
            if (log.isWarnEnabled()) {
                log.warn("cannot change the sourceID while InputSourceGenerator is running");
            }
            return;
        }
        this.sourceID = sourceID;
        if (log.isInfoEnabled()) {
            log.info("sourceID = " + sourceID);
        }
    }

    public int getSourceID() {
        return sourceID;
    }

    public void setTriggerMode(int triggerMode) {
        if (isRunning) {
            if (log.isWarnEnabled()) {
                log.warn("cannot change triggerMode while InputSourceGenerator is running");
            }
            return;
        }
        this.triggerMode = triggerMode;
        if (log.isInfoEnabled()) {
            log.info("Trigger Mode = " + triggerMode);
        }
    }

    public int getTriggerMode() {
        return triggerMode;
    }

    public void setPayloadType(String payloadType) {
        if (isRunning) {
            if (log.isWarnEnabled()) {
                log.warn("cannot create HitGenerator/HitDataGenerator while InputSourceGenerator is running");
            }
            return;
        }
        if (payloadType == null) {
            if (log.isErrorEnabled()) {
                log.error("Payload type cannot be null");
            }
            throw new IllegalArgumentException("Payload type cannot be null");
        } else if (payloadType.equalsIgnoreCase(HIT_DATA_PAYLOAD)) {
            generator = new HitDataGenerator();
            if (log.isInfoEnabled()) {
                log.info("initialized IGenerator with HitDataGenerator");
            }
            this.payloadType = payloadType;
        } else if (payloadType.equalsIgnoreCase(HIT_PAYLOAD)) {
            generator = new icecube.daq.sim.HitGenerator();
            if (log.isInfoEnabled()) {
                log.info("initialized IGenerator with HitDataGenerator");
            }
            this.payloadType = payloadType;
        } else {
            if (log.isErrorEnabled()) {
                log.error("Unknown Payload type");
            }
            throw new IllegalArgumentException("Unknown Payload type");
        }
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setProcessDuration(int minutes) {
        this.minutes = minutes;
        if (log.isInfoEnabled()) {
            log.info("Process Duration Time = " + minutes + " minutes");
        }
    }

    public int getProcessDuration() {
        return minutes;
    }

    /**
     * return an array of SelectableChannel(s) generated by this object
     *
     * @return an array of SelectableChannel
     */
    public SelectableChannel getSourceChannel() {
        return sourceChannel;
    }

    /**
     * start processing data by reading them from the source and feeding the other end of the pipe
     *
     * @throws java.io.IOException
     */
    public void startProcessing() throws IOException {
        isRunning = true;
        if (log.isInfoEnabled()) {
            log.info("starting InputSourceGenerator");
        }
        new Thread(new InputGenerator(seed)).start();
    }

    /**
     * stop processing data
     *
     * @throws IOException
     */
    public void stopProcessing() throws IOException {
        isRunning = false;
        if (log.isInfoEnabled()) {
            log.info("Stopping processing " + toString());
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

     /**
     * Set number of hits
     * @param hits
     */
    public void setNumOfHits(long hits){
        this.totalHits = hits;
    }

    /**
     * get the number of hits
     * @return a long value
     */
    public long getNumOfHits(){
        return totalHits;
    }

    public void generatePayload() {
        long currentTime = System.currentTimeMillis();
        long domId = (long) domGenerator.nextInt(numDoms);
        sendPayload(new GenericHit(currentTime, domId, sourceID, triggerMode));
    }

    // overwrite toString() to fully describe InputSourceGenerator info
    public String toString() {
        return "rate: " + rate + " num of doms: " + numDoms + " sourceID: " + sourceID + " triggerMode: " +
                triggerMode + " numHits: " + numHits;
    }

    private void sendPayload(GenericHit hit){
        if (null != hit) {
            ++numHits;
            try {
                ByteBuffer buf = generator.generatePayload(hit);
                buf.clear();
                int header = buf.getInt(0);
                buf.limit(header);
                buf.position(0);
                if (buf.getInt(0) < 4) {
                    throw new RuntimeException("Problem while generating hits " + header);
                }
                int result = sinkChannel.write(buf);
                if (result != header) {
                    throw new RuntimeException("SinkChannel failed to write, expected " +
                            header + " got " + result);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Generated HitPayload with timestamp = " + hit.getTimeStamp());
                }
            } catch (IOException e) {
                log.fatal("ERROR while writing output", e);
                isRunning = false;
                sendStopSignal();
            }
        } else {
            isRunning = false;
            if (log.isWarnEnabled()) {
                log.warn("ISource generated a null GenericHit");
            }
            sendStopSignal();
        }
    }

    public void sendStopSignal() {
        int done = 4;
        ByteBuffer buf = ByteBuffer.allocate(done);
        buf.clear();
        buf.limit(done);
        buf.putInt(0, done);
        try {
            buf.position(0);
            sinkChannel.write(buf);
            if (log.isInfoEnabled()) {
                log.info(InputSourceGenerator.this.toString() + " --- STOPPED");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        isRunning = false;
    }

    // This is the class that will generate the input data and feed them to the source channels
    class InputGenerator implements Runnable {

        private Long seed;

        public InputGenerator() {
            this(null);
        }

        public InputGenerator(Long seed) {
            this.seed = seed;
        }

        // run this generator
        public void run() {
            ISource hitSource = null;
            if (seed == null) {
                hitSource = new RandomSource(rate, numDoms, sourceID, triggerMode);
            } else {
                hitSource = new RandomSource(rate, numDoms, sourceID, triggerMode, seed.longValue());
            }
            if (minutes < 1){
                log.info("Will generate " + totalHits + " hits from " + numDoms + " DOMs, at a rate of "
                         + rate + " per tenth of nanosecond per DOM");
                while (isRunning && (numHits < totalHits)) {
                    GenericHit hit = (GenericHit) hitSource.nextPayload();
                    sendPayload(hit);
                }
            } else {
                long currentTime = System.currentTimeMillis();
                long duration = minutes * 60 * 1000;
                log.info("Generator will run for " + minutes + " minutes");
                long totalTime = currentTime + duration;
                while(isRunning && (totalTime > currentTime)){
                    GenericHit hit = (GenericHit) hitSource.nextPayload();
                    sendPayload(hit);
                    currentTime = System.currentTimeMillis();
                }
            }
            sendStopSignal();
        }
    }

    public static void main(String[] args) {
        try {
            InputSourceGenerator isg = new InputSourceGenerator();
            isg.startProcessing();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
