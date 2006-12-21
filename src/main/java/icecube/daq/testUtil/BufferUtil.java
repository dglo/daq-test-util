/*
 * class: BufferUtil
 *
 * Version $Id: BufferUtil.java,v 1.6 2005/06/01 23:54:03 mcp Exp $
 *
 * Date: February 15 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.testUtil;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * This class ...does what?
 *
 * @author mcp
 * @version $Id: BufferUtil.java,v 1.6 2005/06/01 23:54:03 mcp Exp $
 */
public class BufferUtil {

    public static final int DEFAULT_MAX_BUFFERS = 1000;
    public static final int DEFAULT_BUFFER_BLEN = 32000;
    public static final int DEFAULT_HEADER_BLEN = 32;
    public static final int INT_SIZE = 4;
    public static final int COMPARE_SUCCESSFUL = 0;
    public static final int HEADER_COMPARE_ERROR = 1;
    public static final int BODY_COMPARE_ERROR = 2;
    public static final int HEADER_FORMAT_ERROR = 3;
    public static final int BODY_FORMAT_ERROR = 4;

    public static final int BODY_PATTERN_ZERO = 0;
    public static final int BODY_PATTERN_ONE = 1;
    public static final int BODY_PATTERN_ALTERNATING_BITS = 2;
    public static final int BODY_PATTERN_INC = 3;
    public static final int BODY_PATTERN_RANDOM = 4;
    public static final int DEFAULT_BODY_PATTERN = BODY_PATTERN_INC;

    private int localSeqNum = 0;
    private static final int SEQ_NUMBER_INDEX = 8;
    private static final int RANDOM_SUM_INDEX = 16;
    private static final int LAST_MSG_FLAG_INDEX = 20;
    private static final int LAST_MESSAGE = 1;
    private static final int NOT_LAST_MESSAGE = 0;
 
    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public BufferUtil() {
        localSeqNum = 0;
    }

    // instance member method (alphabetic)
    public  int requiredRawBufferBlen(int desiredBodyBlen) {
        return desiredBodyBlen +
                DEFAULT_HEADER_BLEN + INT_SIZE + INT_SIZE + INT_SIZE;
    }

    public   int availableBodyBlen(ByteBuffer buffer) {
        return buffer.capacity() -
                (DEFAULT_HEADER_BLEN + INT_SIZE + INT_SIZE + INT_SIZE);
    }

    public   void createBuffer(ByteBuffer buffer) throws Exception {
        if (buffer.capacity() < DEFAULT_HEADER_BLEN + INT_SIZE + INT_SIZE + INT_SIZE) {
            throw new Exception("BufferUtil: buffer too small");
        }
        buffer.position(0);
        buffer.putInt(buffer.capacity());
    }

    public  void createAndFillDefaultBuffer(ByteBuffer buffer) throws Exception {
        createAndFillBuffer(buffer, availableBodyBlen(buffer),
                localSeqNum, DEFAULT_BODY_PATTERN);
        localSeqNum++;
    }

    public   void createAndFillBuffer(ByteBuffer buffer,
                                           int bodyBlen, int seqNum, int type) throws Exception {
        if (buffer.capacity() < DEFAULT_HEADER_BLEN + INT_SIZE +
                INT_SIZE + INT_SIZE + bodyBlen) {
            throw new Exception("BufferUtil: buffer too small");
        }

        buffer.position(0);
        buffer.putInt(buffer.capacity());
        buffer.position(0);
        fillBufferHeader(buffer, seqNum, type);
        fillBufferBody(buffer, bodyBlen);
    }

    public   void fillBufferHeader(ByteBuffer buffer,
                                        int seqNum, int type) {
        buffer.position(INT_SIZE);
        buffer.putInt(DEFAULT_HEADER_BLEN);
        buffer.putInt(seqNum);
        buffer.putInt(type);
        buffer.putInt(LAST_MSG_FLAG_INDEX, NOT_LAST_MESSAGE);
    }

    public   void fillBufferBody(ByteBuffer buffer, int bodyBlen) throws Exception {
        if (bodyBlen > (buffer.getInt(0) - buffer.getInt(INT_SIZE) - (3 * INT_SIZE))) {
            throw new Exception("Buffer body length too small");
        }
        buffer.position(3 * INT_SIZE);
        int type = buffer.getInt();

        int hdrBlen = buffer.getInt(INT_SIZE);
        buffer.position(INT_SIZE + INT_SIZE + buffer.getInt(INT_SIZE));
        buffer.putInt(bodyBlen);
        switch (type) {
            case BODY_PATTERN_ZERO:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        buffer.put((byte) 0);
                    }
                }
                break;
            case BODY_PATTERN_ONE:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        buffer.put((byte) 1);
                    }
                }
                break;
            case BODY_PATTERN_INC:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        buffer.put((byte) i);
                    }
                }
                break;
            case BODY_PATTERN_ALTERNATING_BITS:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        buffer.put((byte) 0xcc);
                    }
                }
                break;
            case BODY_PATTERN_RANDOM:
                {
                    int sum = 0;
                    for (int i = 0; i < bodyBlen; i++) {
                        Random r = new Random();
                        byte b = (byte) r.nextInt();
                        sum += (int) b;
                        byte ir = (byte) r.nextInt();
                    }
                    buffer.putInt(RANDOM_SUM_INDEX, sum);
                }
                break;
        }
    }

    public int bufferBlen(ByteBuffer buffer) {
        return buffer.getInt(0);
    }

    public  int compareBuffers(ByteBuffer buffer0, ByteBuffer buffer1) {
        int sts = compareBufferHeaders(buffer0, buffer1);
        if (sts != COMPARE_SUCCESSFUL) {
            return sts;
        }
        sts = compareBufferBodys(buffer0, buffer1);
        if (sts != COMPARE_SUCCESSFUL) {
            return sts;
        }
        return sts;
    }

    public  int compareBufferHeaders(ByteBuffer buffer0, ByteBuffer buffer1) {
        int buffer0_blen = buffer0.getInt(0);
        int buffer1_blen = buffer1.getInt(0);
        int buffer0_header_blen = buffer0.getInt(INT_SIZE);
        int buffer1_header_blen = buffer1.getInt(INT_SIZE);
        if (buffer0_blen != buffer1_blen) {
            return HEADER_COMPARE_ERROR;
        }
        if (buffer0_header_blen != buffer1_header_blen) {
            return HEADER_COMPARE_ERROR;
        }
        if (buffer0_blen < INT_SIZE + buffer0_header_blen) {
            return HEADER_FORMAT_ERROR;
        }
        if (buffer1_blen < INT_SIZE + buffer1_header_blen) {
            return HEADER_FORMAT_ERROR;
        }
        for (int i = 0; i < buffer0_header_blen; i++) {
            if (buffer0.get(INT_SIZE + i) !=
                    buffer1.get(INT_SIZE + i)) {
                return HEADER_COMPARE_ERROR;
            }
        }
        return COMPARE_SUCCESSFUL;
    }

    public  int compareBufferBodys(ByteBuffer buffer0, ByteBuffer buffer1) {
        int buffer0_blen = buffer0.getInt(0);
        int buffer1_blen = buffer1.getInt(0);
        int buffer0_body_blen = buffer0.getInt(INT_SIZE);
        int buffer1_body_blen = buffer1.getInt(INT_SIZE);
        if (buffer0_blen != buffer1_blen) {
            return BODY_COMPARE_ERROR;
        }
        if (buffer0_body_blen != buffer1_body_blen) {
            return BODY_COMPARE_ERROR;
        }
        if (buffer0_blen < INT_SIZE + buffer0_body_blen) {
            return BODY_FORMAT_ERROR;
        }
        if (buffer1_blen < INT_SIZE + buffer1_body_blen) {
            return BODY_FORMAT_ERROR;
        }
        for (int i = 0; i < buffer0_body_blen; i++) {
            if (buffer0.get(INT_SIZE + i) !=
                    buffer1.get(INT_SIZE + i)) {
                return BODY_COMPARE_ERROR;
            }
        }
        return COMPARE_SUCCESSFUL;
    }

    public  boolean verifyBufferContents(ByteBuffer buffer) {
        buffer.position(4 * INT_SIZE);
        int type = buffer.getInt();
        buffer.position(INT_SIZE + buffer.getInt(INT_SIZE));
        int bodyBlen = buffer.getInt();
        switch (type) {
            case BODY_PATTERN_ZERO:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        if (buffer.getChar() != (char) 0) {
                            return false;
                        }
                    }
                }
            case BODY_PATTERN_ONE:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        if (buffer.getChar() != (char) 1) {
                            return false;
                        }
                    }
                }
            case BODY_PATTERN_INC:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        if (buffer.getChar() != (char) i) {
                            return false;
                        }
                    }
                }
            case BODY_PATTERN_ALTERNATING_BITS:
                {
                    for (int i = 0; i < bodyBlen; i++) {
                        if (buffer.getChar() != (char) 0xcc) {
                            return false;
                        }
                    }
                }
            case BODY_PATTERN_RANDOM:
                {
                    int sum = 0;
                    for (int i = 0; i < bodyBlen; i++) {
                        sum += (int) buffer.getChar();
                    }
                    if (sum != buffer.getInt(RANDOM_SUM_INDEX))
                        buffer.putInt(RANDOM_SUM_INDEX, sum);
                }
                break;
        }

        return true;
    }

    public  int getSequenceNumber(ByteBuffer buffer) {
        return buffer.getInt(SEQ_NUMBER_INDEX);
    }

    public  void setSequenceNumber(ByteBuffer buffer, int seqNumber) {
        buffer.putInt(SEQ_NUMBER_INDEX, seqNumber);
    }

    public  void setLastMsgFlag(ByteBuffer buffer) {
        buffer.putInt(LAST_MSG_FLAG_INDEX, LAST_MESSAGE);
    }

    public  void setNotLastMsgFlag(ByteBuffer buffer) {
        buffer.putInt(LAST_MSG_FLAG_INDEX, NOT_LAST_MESSAGE);
    }

    public  boolean isLastMessage(ByteBuffer buffer) {
        if(buffer.getInt(LAST_MSG_FLAG_INDEX) == 1) {
            return true;
        }
        else {
            return false;
        }
    }
}