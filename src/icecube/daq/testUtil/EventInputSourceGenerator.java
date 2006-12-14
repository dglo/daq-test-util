/**
 * Version $Id: EventInputSourceGenerator.java,v 1.12 2006/01/03 04:16:51 dglo Exp $
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import icecube.daq.sim.GenericHit;
import icecube.daq.sim.GenericTriggerRequest;
import icecube.daq.sim.HitDataGenerator;
import icecube.daq.sim.HitGenerator;
import icecube.daq.sim.IGenerator;
import icecube.daq.sim.ISource;
import icecube.daq.sim.RandomTriggerRequest;
import icecube.daq.sim.TriggerRequestGenerator;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Generate input sources for event builder.
 */
public class EventInputSourceGenerator
    implements InputSource
{
    private static final String HIT_DATA_PAYLOAD = "HitDataPayload";
    private static final String HIT_PAYLOAD = "HitPayload";

    private static final int SRCTYPE_HIT = 1;
    private static final int SRCTYPE_TRIGREQ = 2;

    private static final Log log =
        LogFactory.getLog(EventInputSourceGenerator.class);

    private RandomTriggerRequest trigReqSrc;
    private TriggerRequestGenerator trigReqGen;

    private ISource[] hitSrcs;
    private IGenerator hitGen;

    private boolean isRunning;
    private boolean hasRun;
    private int minutes;
    private long seed;
    private long totalGenerated;

    private double rate = 0.0000001;
    private int numDoms = 60;
    private int sourceId = 4000;
    private int triggerMode = 2;
    private long maxGenerated = 1000;
    private int maxHitsPerTrigger = 10;
    private boolean writeHits = true;

    private String hitPayloadType = HIT_PAYLOAD;

    private Pipe.SourceChannel trigReqSrcChan;
    private Pipe.SinkChannel trigReqSinkChan;

    private Pipe.SourceChannel[] hitSrcChan;
    private Pipe.SinkChannel[] hitSinkChan;

    /** time of previous payload */
    private long prevPayloadTime;

    public EventInputSourceGenerator()
        throws IOException
    {
        this(10); 
    }

    public EventInputSourceGenerator(int maxHitsPerTrigger)
        throws IOException
    {
        this(new RandomTriggerRequest(), maxHitsPerTrigger); 
    }

    public EventInputSourceGenerator(long randomSeed, int maxHitsPerTrigger)
        throws IOException
    {
        this(new RandomTriggerRequest(randomSeed), maxHitsPerTrigger); 
    }

    private EventInputSourceGenerator(RandomTriggerRequest trigReqSrc,
                                      int maxHitsPerTrigger)
        throws IOException
    {
        this.trigReqSrc = trigReqSrc;
        this.maxHitsPerTrigger = maxHitsPerTrigger;

        trigReqGen = new TriggerRequestGenerator();

        hitSrcs = trigReqSrc.getHitSources();
        hitGen = null;

        hitSrcChan = new Pipe.SourceChannel[hitSrcs.length];
        hitSinkChan = new Pipe.SinkChannel[hitSrcs.length];
        for (int i = 0; i < hitSrcs.length; i++) {
            Pipe hitPipe = Pipe.open();
            hitSinkChan[i] = hitPipe.sink();
            hitSinkChan[i].configureBlocking(true);

            hitSrcChan[i] = hitPipe.source();
            hitSrcChan[i].configureBlocking(false);
        }

        Pipe trPipe = Pipe.open();
        trigReqSinkChan = trPipe.sink();
        trigReqSrcChan = trPipe.source();
        trigReqSinkChan.configureBlocking(true);
        trigReqSrcChan.configureBlocking(false);

        if (log.isInfoEnabled()) {
            log.info("Created " + toString());
        }
    }

    private void generatePayload()
    {
        GenericTriggerRequest trigReq =
            (GenericTriggerRequest) trigReqSrc.nextPayload();
        if (trigReq == null) {
            isRunning = false;
            if (log.isWarnEnabled()) {
                log.warn("Trigger request source returned null");
            }

            try {
                sendStopSignal();
            } catch (IOException ioe) {
                log.error("Stop signal failed", ioe);
            }
        } else {
            sendTriggerRequestPayload(trigReq);

            // return a series of hits equally spaced across
            // the trigger request time window

            for (int i = 0; i < hitSrcs.length; i++) {
                while (true) {
                    GenericHit hit = (GenericHit) hitSrcs[i].nextPayload();
                    if (hit == null) {
                        break;
                    }

                    if (writeHits) {
                        sendHitPayload(hit, hitSinkChan[i]);
                    }
                }
            }
        }

        totalGenerated++;
    }

    /**
     * Return an array of hit sources.
     *
     * @return hit sources
     */
    public InputSource[] getHitSources()
    {
        InputSource[] srcs = new InputSource[hitSinkChan.length];
        for (int i = 0; i < hitSinkChan.length; i++) {
            srcs[i] = new EventInputChannelSource(this, hitSrcChan[i],
                                                  hitSinkChan[i]);
        }
        return srcs;
    }

    /**
     * Get the maximum number of hits returned for a single trigger.
     *
     * @return max number of hits per trigger
     */
    public int getMaxHitsPerTrigger()
    {
        return maxHitsPerTrigger;
    }

    /**
     * Get the number of DOMs for this input source.
     *
     * @return number of DOMs
     */
    public int getNumDoms()
    {
        return numDoms;
    }
 
    /**
     * Get the number of triggers to be generated.
     *
     * @return maximum number of triggers
     */
    public long getNumOfHits()
    {
        return maxGenerated;
    }

    /**
     * Get payload type.
     *
     * @return payload type
     */
    public String getPayloadType()
    {
        return hitPayloadType;
    }

    /**
     * Get the duration of the process in minutes.
     *
     * @return duration
     */
    public int getProcessDuration()
    {
        return minutes;
    }

    /**
     * Get the rate.
     *
     * @return rate
     */
    public double getRate()
    {
        return rate;
    }

    /**
     * Return an array of SelectableChannel(s) generated by this object.
     *
     * @return an array of SelectableChannel
     */
    public SelectableChannel getSourceChannel()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Get the source ID.
     *
     * @return source ID
     */
    public int getSourceID()
    {
        return sourceId;
    }

    /**
     * Get the trigger mode.
     *
     * @return trigger mode
     */
    public int getTriggerMode()
    {
        return triggerMode;
    }

    /**
     * Return the trigger request source channel.
     *
     * @return trigger request source channel
     */
    public InputSource getTriggerRequestSource()
    {
        return new EventInputChannelSource(this, trigReqSrcChan,
                                           trigReqSinkChan);
    }

    /**
     * Initialize hit generator based on hitPayloadType.
     */
    private void initHitGenerator()
    {
        if (hitPayloadType == null) {
            throw new Error("Hit payload has not been specified");
        } else if (hitPayloadType.equals(HIT_PAYLOAD)) {
            hitGen = new HitGenerator();
        } else if (hitPayloadType.equals(HIT_DATA_PAYLOAD)) {
            hitGen = new HitDataGenerator();
        } else {
            throw new Error("Unknown hit payload type '" + hitPayloadType +
                            "'");
        }
    }

    /**
     * Is the input source running?
     *
     * @return <tt>true</tt> if input source is running.
     */
    public boolean isRunning()
    {
        return isRunning;
    }

    public void reset()
    {
        if (!isRunning) {
            hasRun = false;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot reset while generator is running");
        }
    }

    private void sendHitPayload(GenericHit hit, Pipe.SinkChannel sinkChan)
    {
        try {
            sendPayload(hitGen.generatePayload(hit), sinkChan);
        } catch (IOException ioe) {
            throw new RuntimeException("While sending hit: ", ioe);
        }

        if (log.isDebugEnabled()) {
            log.debug("Generated HitPayload with timestamp " +
                      hit.getTimeStamp());
        }
    }

    private void sendPayload(ByteBuffer buf, Pipe.SinkChannel sinkChan)
        throws IOException
    {
        buf.clear();

        int len = buf.getInt(0);
        buf.limit(len);
        buf.position(0);
        if (buf.getInt(0) < 4) {
            throw new IOException("Bad payload length " + len);
        }

        int result = sinkChan.write(buf);
        if (result != len) {
            throw new IOException("SinkChannel write failed, expected " +
                                  len + " bytes, but wrote " + result);
        }
    }

    private void sendTriggerRequestPayload(GenericTriggerRequest trigReq)
    {
        try {
            sendPayload(trigReqGen.generatePayload(trigReq), trigReqSinkChan);
        } catch (IOException ioe) {
            throw new RuntimeException("While sending trigger request: ", ioe);
        }

        if (log.isDebugEnabled()) {
            log.debug("Generated TriggerRequestPayload with" +
                      " UID " + trigReq.getTriggerUID() +
                      " timestamp " + trigReq.getTimeStamp());
        }
    }

    public void sendStopSignal()
        throws IOException
    {
        final int doneLen = 4;

        ByteBuffer buf = ByteBuffer.allocate(doneLen);
        buf.clear();
        buf.limit(doneLen);
        buf.putInt(0, doneLen);

        buf.position(0);
        trigReqSinkChan.write(buf);

        if (writeHits) {
            for (int i = 0; i < hitSinkChan.length; i++) {
                buf.position(0);
                hitSinkChan[i].write(buf);
            }
        }

        if (log.isInfoEnabled()) {
            log.info(toString() + " --- STOPPED");
        }

        isRunning = false;
    }

    /**
     * Set the maximum number of hits returned for a single trigger.
     *
     * @param maxHits max number of hits per trigger
     */
    public void setMaxHitsPerTrigger(int maxHits)
    {
        maxHitsPerTrigger = maxHits;
    }

    /**
     * Set the number of DOMs.
     *
     * @param numDoms number of DOMs
     */
    public void setNumDoms(int numDoms)
    {
        if (!isRunning) {
            this.numDoms = numDoms;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the rate" +
                     " while generator is running");
        }
    }

    /**
     * Set the number of triggers to be generated.
     *
     * @param numGen number of triggers to be generated
     */
    public void setNumOfHits(long numGen)
    {
        if (!isRunning) {
            this.maxGenerated = numGen;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the maximum number generated" +
                     " while generator is running");
        }
    }

    /**
     * Set the payload type
     *
     * @param payloadType
     */
    public void setPayloadType(String payloadType)
    {
        if (!isRunning) {
            if (payloadType != null &&
                (payloadType.equals(HIT_DATA_PAYLOAD) ||
                 payloadType.equals(HIT_PAYLOAD)))
            {
                hitPayloadType = payloadType;
                hitGen = null;
            } else if (log.isErrorEnabled()) {
                log.error("Illegal payload type '" + payloadType + "'");
            }
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the process duration" +
                     " while generator is running");
        }
    }

    /**
     * Set the duration of the process in minutes.
     *
     * @param minutes
     */
    public void setProcessDuration(int minutes)
    {
        if (!isRunning) {
            this.minutes = minutes;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the process duration" +
                     " while generator is running");
        }
    }

    /**
     * Set the rate.
     *
     * @param rate
     */
    public void setRate(double rate)
    {
        if (!isRunning) {
            this.rate = rate;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the rate" +
                     " while generator is running");
        }
    }

    /**
     * Set the source ID.
     *
     * @param sourceId
     */
    public void setSourceID(int sourceId)
    {
        if (!isRunning) {
            this.sourceId = sourceId;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the source ID" +
                     " while generator is running");
        }
    }

    /**
     * Set the list of readout source IDs.
     *
     * @param srcIds array of integer source IDs
     */
    public void setTargetSourceIds(int[] srcIds)
    {
        trigReqSrc.setTargetSourceIds(srcIds);
    }

    /**
     * Set the list of readout source IDs.
     *
     * @param srcIds collection of ISourceIDs
     */
    public void setTargetSourceIds(Collection srcIds)
    {
        trigReqSrc.setTargetSourceIds(srcIds);
    }

    /**
     * Set the trigger mode.
     *
     * @param triggerMode
     */
    public void setTriggerMode(int triggerMode)
    {
        if (!isRunning) {
            this.triggerMode = triggerMode;
        } else if (log.isWarnEnabled()) {
            log.warn("cannot change the trigger mode" +
                     " while generator is running");
        }
    }

    /**
     * Should hits be written to the output channel?
     *
     * @param val <tt>false</tt> if hits should not be written
     */
    public void setWriteHits(boolean val)
    {
        writeHits = val;
    }

    /**
     * Start processing data by reading them from the source
     * and feeding the other end of the pipe.
     *
     * @throws IOException
     */
    public synchronized void startProcessing()
        throws IOException
    {
        if (!trigReqSrc.isInitialized()) {
            throw new IllegalArgumentException("List of target SourceIDs" +
                                               " has not been specified");
        }

        if (!isRunning && !hasRun) {
            initHitGenerator();

            isRunning = true;
            if (log.isInfoEnabled()) {
                log.info("starting " + toString());
            }

            Thread thread = new Thread(new InputGenerator());
            thread.setName("EventInputSourceGenerator");
            thread.start();

            hasRun = true;
        }
    }

    /**
     * Stop processing data.
     *
     * @throws IOException
     */
    public void stopProcessing()
        throws IOException
    {
        if (isRunning) {
            isRunning = false;

            if (log.isInfoEnabled()) {
                log.info("Stopping processing " + toString());
            }
        }
    }

    /**
     * String description of internal state.
     *
     * @return internal state string
     */
    public String toString()
    {
        return "EventInputSourceGenerator";
    }

    /**
     * Input generator thread.
     */
    class InputGenerator
        implements Runnable
    {
        public InputGenerator()
        {
        }

        private void countLoop()
        {
            log.info("Will generate " + maxGenerated +
                     " triggers from " + numDoms +
                     " DOMs, with hits at a rate of " + rate +
                     " per tenth of nanosecond per DOM");

            long numGenerated = 0;
            while (isRunning && (numGenerated < maxGenerated)) {
                generatePayload();
                numGenerated++;
            }
        }

        private void timedLoop()
        {
            log.info("Will generate " + minutes +
                     " minutes of triggers from " + numDoms +
                     " DOMs, with hits at a rate of " + rate +
                     " per tenth of nanosecond per DOM");

            long currentTime = System.currentTimeMillis();

            final long stopTime = System.currentTimeMillis() +
                (long) (minutes * 60000);
            while (isRunning && (System.currentTimeMillis() < stopTime)) {
                generatePayload();
            }
        }

        public void run()
        {
            if (minutes <= 0) {
                countLoop();
            } else {
                timedLoop();
            }

            try {
                sendStopSignal();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    class EventInputChannelSource
        implements InputSource
    {
        private EventInputSourceGenerator evtSrc;
        private SelectableChannel srcChan;
        private SelectableChannel sinkChan;

        EventInputChannelSource(EventInputSourceGenerator evtSrc,
                                SelectableChannel srcChan,
                                SelectableChannel sinkChan)
        {
            this.evtSrc = evtSrc;
            this.srcChan = srcChan;
            this.sinkChan = sinkChan;
        }

        public int getNumDoms() { return evtSrc.getNumDoms(); }
        public long getNumOfHits() { return evtSrc.getNumOfHits(); }
        public String getPayloadType() { return evtSrc.getPayloadType(); }
        public int getProcessDuration() { return evtSrc.getProcessDuration(); }
        public double getRate() { return evtSrc.getRate(); }

        /**
         * Return the appropriate source channel.
         *
         * @return appropriate source channel
         */
        public SelectableChannel getSourceChannel()
        {
            return srcChan;
        }

        public int getSourceID() { return evtSrc.getSourceID(); }
        public int getTriggerMode() { return evtSrc.getTriggerMode(); }

        public boolean isRunning() { return evtSrc.isRunning(); }

        public void setNumDoms(int xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setNumOfHits(long xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setPayloadType(String xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setProcessDuration(int xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setRate(double xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setSourceID(int xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void setTriggerMode(int xxx)
        {
            throw new RuntimeException("Please use " +
                                       evtSrc.getClass().getName() +
                                       " to set attributes");
        }

        public void startProcessing()
            throws IOException
        {
            evtSrc.startProcessing();
        }

        public void stopProcessing()
            throws IOException
        {
            evtSrc.stopProcessing();
        }
    }

    public static final void main(String[] args)
        throws Exception
    {
        EventInputSourceGenerator gen = new EventInputSourceGenerator();
        gen.startProcessing();
    }
}
