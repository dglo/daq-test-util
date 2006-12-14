/**
 * FileReaderManager
 * Date: Jun 30, 2005 12:04:48 PM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.testUtil;

import icecube.daq.payload.*;
import icecube.daq.splicer.SpliceableFactory;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is class manages creates multiple input sources from one input file and
 * manages reading from multiple sources in the same order as they were splitted.
 *
 * @author artur
 * @version $Id: FileReaderManager.java,v 1.12 2006/06/30 18:07:02 dwharton Exp $
 */
public class FileReaderManager {

    private List fileReaders;
    private String sourceFileName;
    private int numSubFiles = 0;
    private Random random;
    private IByteBufferCache bufferCacheMgr;
    private final int MAX_SLEEP_TIME = 20;
    private boolean isRunning = false;
    private PayloadReader payloadReader;
    private static SpliceableFactory sFac = new MasterPayloadFactory();
    private Log log = LogFactory.getLog(FileReaderManager.class);

    public int numPayloads = 0;

    public FileReaderManager(String sourceFileName, int numSubFiles) throws IOException {

        if (sourceFileName == null) {
            throw new IllegalArgumentException("SourceFileName cannot be null");
        }
        this.sourceFileName = sourceFileName;

        payloadReader = new PayloadReader(sourceFileName);

        this.numSubFiles = numSubFiles;

        random = new Random();

        fileReaders = new ArrayList();

		bufferCacheMgr = new ByteBufferCache(1024,"FileReaderManager");

        init();
    }

    // make sure we init all channel readers
    private void init() throws IOException {
        File sourceFile = new File(sourceFileName);
        if (!sourceFile.exists()) {
            throw new IOException(sourceFileName + " does not exist.");
        }

        // no splitting files
        if (numSubFiles <= 1) {
            FileReaderChannel frc = new FileReaderChannel(sourceFileName, 0);
            fileReaders.add(frc);
            if (log.isInfoEnabled()) {
                log.info("Using a single channel for " + sourceFileName);
            }
        } else {
            // create FileReaderChannel and add them to the list
            List subFileChannels = new ArrayList();
            for (int i = 0; i < numSubFiles; i++) {
                File subFile = new File(sourceFile.getParentFile(), (i + sourceFile.getName()));
                if (subFile.exists()) {
                    subFile.delete();
                }
                FileChannel wChannel = new FileOutputStream(subFile, true).getChannel();
                subFileChannels.add(wChannel);
                FileReaderChannel frc = new FileReaderChannel(subFile.getAbsolutePath(), i);
                fileReaders.add(frc);
            }

            payloadReader.open();
            int numRead = 1;
            int subFileChannelIndex = 0;
            // now, read from the main source file and write to the temp files
            while (numRead > 0) {
                ByteBuffer buf = bufferCacheMgr.acquireBuffer(BufferUtil.DEFAULT_BUFFER_BLEN);
                if (buf == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        //ignore this for now
                    }
                    continue;
                }
                try {
                    buf.clear();
                    numRead = payloadReader.readNextPayload(buf);
                } catch (EOFException e) {
                    if (log.isInfoEnabled()) {
                        log.info("End of Initialization");
                    }
                    bufferCacheMgr.returnBuffer(buf);
                    
                    return;
                } catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.info("What happend here? " + e.getMessage());
                    }
                }
                if (numRead < 1) {

                    bufferCacheMgr.returnBuffer(buf);
                    break;
                }
                int header = buf.getInt(0);
                buf.limit(header);
                buf.position(0);
                FileChannel wChannel = (FileChannel) subFileChannels.get(subFileChannelIndex);
                wChannel.write(buf);
                numPayloads++;
                if (subFileChannelIndex < subFileChannels.size() - 1) {
                    ++subFileChannelIndex;
                } else {
                    subFileChannelIndex = 0;
                }
                bufferCacheMgr.returnBuffer(buf);
            }

            for (int i = 0; i < subFileChannels.size(); i++) {
                FileChannel wChannel = (FileChannel) subFileChannels.get(i);
                wChannel.close();
            }
            subFileChannels.clear();
            payloadReader.close();
            if (log.isInfoEnabled()) {
                log.info("Using " + numSubFiles + " channels for " + sourceFileName);
            }
        }
    }

    public FileReaderChannel[] getFileReaderChannels() {
        return (FileReaderChannel[]) fileReaders.toArray(new FileReaderChannel[fileReaders.size()]);
    }

    public void startProcessing() throws IOException {
        if (isRunning) {
            return;
        } else {
            isRunning = true;
            int fileReaderIndex = 0;
            int countReaderChannelDone = 0;

            if (fileReaders.size() > 0) {
                while (countReaderChannelDone < fileReaders.size()) {
                    ByteBuffer byteBuffer = bufferCacheMgr.acquireBuffer(BufferUtil.DEFAULT_BUFFER_BLEN);
                    if (byteBuffer == null) {
                        throw new NullPointerException("Cannot start reading with a null BytteBuffer");
                    }

                    FileReaderChannel readerChannel = (FileReaderChannel) fileReaders.get(fileReaderIndex);

                    if (log.isDebugEnabled()) {
                        log.debug("Reading from channel ID = " + fileReaderIndex);
                    }

                    if (readerChannel.isDone()) {
                        readerChannel.stop();
                        ++countReaderChannelDone;
                    } else {
                        readerChannel.read(byteBuffer);
                    }
                    try {
                        Thread.sleep(MAX_SLEEP_TIME);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    bufferCacheMgr.returnBuffer(byteBuffer);
                    if (readerChannel.isDone()) {
                        readerChannel.stop();
                    }
                    if (fileReaderIndex < fileReaders.size() - 1) {
                        ++fileReaderIndex;
                    } else {
                        fileReaderIndex = 0;
                    }
                }
            }
        }
    }

    public void stop() throws IOException {
        for (int i = 0; i < fileReaders.size(); i++) {
            FileReaderChannel frc = (FileReaderChannel) fileReaders.get(i);
            frc.stop();
        }

        isRunning = false;
    }


    public static void main(String[] args) {
        try {
            String sourceFileName = "test_IC1_hardLC.hits.spliced.HitDataPayloads";
            FileReaderManager frm = new FileReaderManager(sourceFileName, 3);

            frm.startProcessing();
            frm.stop();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
