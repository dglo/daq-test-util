/*
 * class: HitGenerator
 *
 * Version $Id: HitGenerator.java,v 1.1 2005/06/23 01:48:57 mcp Exp $
 *
 * Date: June 2 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.testUtil;

import java.nio.ByteBuffer;

/**
 * This class produces a ByteBuffer representation of a HitDataPayload
 *
 * @version $Id: HitGenerator.java,v 1.1 2005/06/23 01:48:57 mcp Exp $
 * @author pat
 */
public class HitGenerator
{

    /**
     * fields with a fixed size (in bytes)
     */
    private static final int PAYLOAD_LENGTH_SIZE = 4;
    private static final int PAYLOAD_TYPE_SIZE   = 4;
    private static final int PAYLOAD_TIME_SIZE   = 8;
    private static final int TRIGGER_CONFIG_SIZE = 4;
    private static final int SOURCE_ID_SIZE      = 4;

    private static final int WRAPPER_LENGTH_SIZE = 4;
    private static final int WRAPPER_ID_SIZE     = 4;
    private static final int DOM_ID_SIZE         = 8;
    private static final int SKIPPED_SIZE        = 8;
    private static final int CALIB_TIME_SIZE     = 8;

    private static final int EVENT_LENGTH_SIZE   = 2;
    private static final int EVENT_FORMAT_SIZE   = 2;
    private static final int MISC_SIZE           = 1;
    private static final int NFADC_SIZE          = 1;
    private static final int ATWD_01_SIZE        = 1;
    private static final int ATWD_23_SIZE        = 1;
    private static final int TRIGGER_SIZE        = 1;
    private static final int SPARE_SIZE          = 1;
    private static final int TIME_STAMP_SIZE     = 6;

    private static final int PAYLOAD_ENVELOPE_SIZE = PAYLOAD_LENGTH_SIZE
                                                     + PAYLOAD_TYPE_SIZE
                                                     + PAYLOAD_TIME_SIZE
                                                     + TRIGGER_CONFIG_SIZE
                                                     + SOURCE_ID_SIZE;
    private static final int WRAPPER_ENVELOPE_SIZE = WRAPPER_LENGTH_SIZE
                                                     + WRAPPER_ID_SIZE
                                                     + DOM_ID_SIZE
                                                     + SKIPPED_SIZE
                                                     + CALIB_TIME_SIZE;
    private static final short EVENT_SIZE_NO_WAVEFORMS = EVENT_LENGTH_SIZE
                                                         + EVENT_FORMAT_SIZE
                                                         + MISC_SIZE
                                                         + NFADC_SIZE
                                                         + ATWD_01_SIZE
                                                         + ATWD_23_SIZE
                                                         + TRIGGER_SIZE
                                                         + SPARE_SIZE
                                                         + TIME_STAMP_SIZE;

    /**
     * offset into buffer for above fields
     */
    private static final int PAYLOAD_LENGTH_OFFSET = 0;
    private static final int PAYLOAD_TYPE_OFFSET   = PAYLOAD_LENGTH_OFFSET + PAYLOAD_LENGTH_SIZE;
    private static final int PAYLOAD_TIME_OFFSET   = PAYLOAD_TYPE_OFFSET   + PAYLOAD_TYPE_SIZE;
    private static final int TRIGGER_CONFIG_OFFSET = PAYLOAD_TIME_OFFSET   + PAYLOAD_TIME_SIZE;
    private static final int SOURCE_ID_OFFSET      = TRIGGER_CONFIG_OFFSET + TRIGGER_CONFIG_SIZE;

    private static final int WRAPPER_LENGTH_OFFSET = SOURCE_ID_OFFSET      + SOURCE_ID_SIZE;
    private static final int WRAPPER_ID_OFFSET     = WRAPPER_LENGTH_OFFSET + WRAPPER_LENGTH_SIZE;
    private static final int DOM_ID_OFFSET         = WRAPPER_ID_OFFSET     + WRAPPER_ID_SIZE;
    private static final int SKIPPED_OFFSET        = DOM_ID_OFFSET         + DOM_ID_SIZE;
    private static final int CALIB_TIME_OFFSET     = SKIPPED_OFFSET        + SKIPPED_SIZE;

    private static final int EVENT_LENGTH_OFFSET   = CALIB_TIME_OFFSET     + CALIB_TIME_SIZE;
    private static final int EVENT_FORMAT_OFFSET   = EVENT_LENGTH_OFFSET   + EVENT_LENGTH_SIZE;
    private static final int MISC_OFFSET           = EVENT_FORMAT_OFFSET   + EVENT_FORMAT_SIZE;
    private static final int NFADC_OFFSET          = MISC_OFFSET           + MISC_SIZE;
    private static final int ATWD_01_OFFSET        = NFADC_OFFSET          + NFADC_SIZE;
    private static final int ATWD_23_OFFSET        = ATWD_01_OFFSET        + ATWD_01_SIZE;
    private static final int TRIGGER_OFFSET        = ATWD_23_OFFSET        + ATWD_23_SIZE;
    private static final int SPARE_OFFSET          = TRIGGER_OFFSET        + TRIGGER_SIZE;
    private static final int TIME_STAMP_OFFSET     = SPARE_OFFSET          + SPARE_SIZE;

    /**
     * payload format defaults
     */
    private static final int PAYLOAD_TYPE_DEFAULT = 10;
    private static final int WRAPPER_ID_DEFAULT   =  2;
    private static final int EVENT_FORMAT_DEFAULT =  2;

    /**
     * waveform format defaults
     */
    private static final int NFADC_DEFAULT   = 56;
    private static final int ATWD_01_DEFAULT = 0x3f;
    private static final int ATWD_23_DEFAULT = 0x03;

    /**
     * payload format
     */
    private int payloadType;
    private int wrapperId;
    private int eventFormat;

    /**
     * waveform format
     */
    private int nFadc;
    private int atwd01;
    private int atwd23;

    /**
     * length variables
     */
    private int fadcLength;
    private int atwd01Length;
    private int atwd23Length;
    private int eventLength;
    private int wrapperLength;
    private int payloadLength;

    /**
     * default constructor
     */
    public HitGenerator() {
        this(NFADC_DEFAULT, ATWD_01_DEFAULT, ATWD_23_DEFAULT);
    }

    /**
     * custom waveform constructor
     * @param nFadc number of fadc samples
     * @param atwd01 atwd format for channels 0 and 1
     * @param atwd23 atwd format for channels 2 and 3
     */
    public HitGenerator(int nFadc, int atwd01, int atwd23) {
        // use default format
        payloadType = PAYLOAD_TYPE_DEFAULT;
        wrapperId   = WRAPPER_ID_DEFAULT;
        eventFormat = EVENT_FORMAT_DEFAULT;

        // use custom waveform
        this.nFadc  = nFadc;
        this.atwd01 = atwd01;
        this.atwd23 = atwd23;

        // calculate lengths
        fadcLength    = calcFadcLength(nFadc);
        atwd01Length  = calcAtwdLength(atwd01);
        atwd23Length  = calcAtwdLength(atwd23);
        eventLength   = EVENT_SIZE_NO_WAVEFORMS + fadcLength + atwd01Length + atwd23Length;
        wrapperLength = WRAPPER_ENVELOPE_SIZE + eventLength;
        payloadLength = PAYLOAD_ENVELOPE_SIZE + wrapperLength;
    }

    /**
     * calculates the length (in bytes) of the fadc field
     * @param nFadc number of fadc samples
     * @return fadc length
     */
    private int calcFadcLength(int nFadc) {
        return 2*nFadc;
    }

    /**
     * calculates the lenght (in bytes) of an atwd pair (01 or 23)
     * @param atwd format of pair
     * @return length
     */
    private int calcAtwdLength(int atwd) {
        int atwdDatum;
        int atwdSamples;
        int atwdLength;

        // first channel of pair
        if ((atwd & 0x01) == 0) {
            atwdDatum = 0;
        } else if ((atwd & 0x02) == 0) {
            atwdDatum = 1;
        } else {
            atwdDatum = 2;
        }

        if ((atwd & 0x04) == 0) {
            if ((atwd & 0x08) == 0) {
                atwdSamples = 32;
            } else {
                atwdSamples = 16;
            }
        } else {
            if ((atwd & 0x08) == 0) {
                atwdSamples = 64;
            } else {
                atwdSamples = 128;
            }
        }
        atwdLength = atwdDatum*atwdSamples;

        // second channel of pair
        if ((atwd & 0x10) == 0) {
            atwdDatum = 0;
        } else if ((atwd & 0x20) == 0) {
            atwdDatum = 1;
        } else {
            atwdDatum = 2;
        }

        if ((atwd & 0x40) == 0) {
            if ((atwd & 0x80) == 0) {
                atwdSamples = 32;
            } else {
                atwdSamples = 16;
            }
        } else {
            if ((atwd & 0x80) == 0) {
                atwdSamples = 64;
            } else {
                atwdSamples = 128;
            }
        }
        atwdLength += atwdDatum*atwdSamples;

        return atwdLength;
    }

    /**
     * generate a hit
     * @param timeStamp corrected time of hit
     * @param domId DOM id of hit
     * @param sourceId source of hit
     * @param triggerMode DOM triggering mode
     * @return ByteBuffer representation of hit
     */
    public ByteBuffer generateHit(long timeStamp, long domId, int sourceId, int triggerMode) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(payloadLength);

        // payload envelope
        byteBuffer.putInt(PAYLOAD_LENGTH_OFFSET, payloadLength);
        byteBuffer.putInt(PAYLOAD_TYPE_OFFSET, payloadType);
        byteBuffer.putLong(PAYLOAD_TIME_OFFSET, timeStamp);
        byteBuffer.putInt(TRIGGER_CONFIG_OFFSET, -1);
        byteBuffer.putInt(SOURCE_ID_OFFSET, sourceId);

        // wrapper
        byteBuffer.putInt(WRAPPER_LENGTH_OFFSET, wrapperLength);
        byteBuffer.putInt(WRAPPER_ID_OFFSET, wrapperId);
        byteBuffer.putLong(DOM_ID_OFFSET, domId);
        byteBuffer.putLong(SKIPPED_OFFSET, 0);
        byteBuffer.putLong(CALIB_TIME_OFFSET, timeStamp);

        // hit
        byteBuffer.putShort(EVENT_LENGTH_OFFSET, new Integer(eventLength).shortValue());
        byteBuffer.putShort(EVENT_FORMAT_OFFSET, new Integer(eventFormat).shortValue());
        byteBuffer.put(MISC_OFFSET, new Integer(0).byteValue());
        byteBuffer.put(NFADC_OFFSET, new Integer(nFadc).byteValue());
        byteBuffer.put(ATWD_01_OFFSET, new Integer(atwd01).byteValue());
        byteBuffer.put(ATWD_23_OFFSET, new Integer(atwd23).byteValue());
        byteBuffer.put(TRIGGER_OFFSET, new Integer(triggerMode).byteValue());
        byteBuffer.put(SPARE_OFFSET, new Integer(0).byteValue());
        byteBuffer.position(TIME_STAMP_OFFSET);
        byteBuffer.put( new byte[6] );

        // waveform
        if (fadcLength > 0) {
            byteBuffer.put( new byte[fadcLength] );
        }
        if (atwd01Length > 0) {
            byteBuffer.put( new byte[atwd01Length] );
        }
        if (atwd23Length > 0) {
            byteBuffer.put( new byte[atwd23Length] );
        }

        // flip buffer before returning it
        byteBuffer.flip();
        return byteBuffer;
    }

}
