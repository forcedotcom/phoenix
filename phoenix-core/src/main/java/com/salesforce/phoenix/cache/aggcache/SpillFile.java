package com.salesforce.phoenix.cache.aggcache;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * This class abstracts a SpillFile It is a accessible on a per page basis
 */
public class SpillFile implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SpillFile.class);
    // Default size for a single spillFile 2GB
    private static final int SPILL_FILE_SIZE = Integer.MAX_VALUE;
    // Page size for a spill file 4K
    static final int DEFAULT_PAGE_SIZE = 4096;

    private File tempFile;
    private FileChannel fc;
    private RandomAccessFile file;
    private int maxPageId;

    /**
     * Create a new SpillFile using the Java TempFile creation function. SpillFile is access in
     * pages.
     */
    public static SpillFile createSpillFile() {
        File tempFile = null;
        try {
            tempFile = File.createTempFile(UUID.randomUUID().toString(), null);
            if (logger.isDebugEnabled()) {
                logger.debug("Creating new SpillFile: " + tempFile.getAbsolutePath());
            }
            RandomAccessFile file = new RandomAccessFile(tempFile, "rw");
            file.setLength(SPILL_FILE_SIZE);
            return new SpillFile(tempFile, file);

        } catch (IOException ioe) {
            throw new RuntimeException("Could not create Spillfile " + ioe);
        }
    }

    private SpillFile(File tempFile, RandomAccessFile file) throws IOException {
        this.tempFile = tempFile;
        this.file = file;
        this.fc = file.getChannel();
        maxPageId = -1;
    }

    /**
     * Returns the next free page within the current spill file
     */
    public MappedByteBuffer getNextFreePage() {
        return getPage(++maxPageId);
    }

    /**
     * Random access to a page of the current spill file
     * @param index
     */
    public MappedByteBuffer getPage(int index) {
        try {
            long offset = (long) index * (long) DEFAULT_PAGE_SIZE;
            Preconditions.checkArgument(offset <= Integer.MAX_VALUE);

            return fc.map(MapMode.READ_WRITE, offset, DEFAULT_PAGE_SIZE);
        } catch (IOException ioe) {
            // Close resource
            close();
            throw new RuntimeException("Could not get page at index: " + index);
        } catch (IllegalArgumentException iae) {
            // Close resource
            close();
            throw iae;
        }
    }

    /**
     * Return the current highest allocaled page in spill file
     */
    public int getMaxPageId() {
        return maxPageId;
    }

    @Override
    public void close() {
        // Swallow IOException
        Closeables.closeQuietly(fc);
        Closeables.closeQuietly(file);

        if (tempFile != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Deleting tempFIle: " + tempFile.getAbsolutePath());
            }
            try {
                tempFile.delete();
            } catch (SecurityException e) {
                logger.warn("IOException thrown while closing Closeable." + e);
            }
            tempFile = null;
        }
    }
}
