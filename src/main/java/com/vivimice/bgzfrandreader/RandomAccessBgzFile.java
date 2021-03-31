package com.vivimice.bgzfrandreader;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * <p>A Random Access BGZF Reader</p>
 * 
 * <p>This class can be used to read uncompressed data of BGZF file at specified offset 
 * relative to data before compression.</p>
 * 
 * <p>BGZF is a GZip compatible file format, it stores block size and input size in extra subfield of each
 * data block, makes random accessing by uncompressed data offset possible. BGZF file can be created by 
 * <code>bgzip</code> command line tool from either uncompressed file or existing .gz file.</p>
 * 
 * <p>Note: Make sure {@link #close()} is called after use. {@link RandomAccessBgzFile} uses {@link Inflater} 
 * to decompress data, which must be closed explicitly. Otherwise unexpected off-heap memory leak may occur.</p>
 * 
 * <p><b>WARNING: This class is not thread safe.</b> Use with caution when shared with multiple threads.</p>
 * 
 * @author vivimice &lt;vivimice@gmail.com&gt;
 * @see http://samtools.github.io/hts-specs/SAMv1.pdf
 */
public class RandomAccessBgzFile implements Closeable, AutoCloseable {

    private final boolean closeChannelOnClose;
    private final SeekableByteChannel channel;
    private final NavigableMap<Long, BgzipBlock> blockTree;
    private final long inputLength;
    private final long basePosition;
    private final Inflater inflater = new Inflater(true);

    private long pos = 0;
    
    private LocalCache preceding = null;
    private LocalCache following = null;
    
    /**
     * <p>Constructs a RandomAccessBgzFile instance using existing {@link RandomAccessFile}.</p>
     * 
     * <p>Note: All following read/seek operations will starts from <code>file</code>'s 
     * current position. Reading or seeking this RandomAccessBgzFile will affect <code>file</code>'s 
     * position, and vice versa.</p>
     * 
     * <p>Note: <code>file</code> won't be closed when calling {@link #close()} method.</p>
     * 
     * @param file
     * @throws IOException If IO error occurs while building index
     * @throws NullPointerException If <code>file</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     */
    public RandomAccessBgzFile(RandomAccessFile file) 
            throws IOException, MalformedBgzipDataException {
        this(file.getChannel(), false);
    }
    
    /**
     * <p>Constructs a RandomAccessBgzFile instance using existing {@link File}.</p>
     * 
     * @param file
     * @throws IOException If IO error occurs while building index
     * @throws NullPointerException If <code>file</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     * @throws FileNotFoundException If <code>file</code> is not a valid file
     * @throws SecurityException If a security manager exists and its checkRead method denies read access to the file.
     */
    @SuppressWarnings("resource")
    public RandomAccessBgzFile(File file) 
            throws IOException, FileNotFoundException, MalformedBgzipDataException {
        this(new FileInputStream(file).getChannel(), true);
    }
    
    /**
     * <p>Constructs a RandomAccessBgzFile instance using existing {@link FileInputStream}.</p>
     * 
     * <p>Note: All following read/seek operations will starts from the number of 
     * bytes read from the file so far. Reading or seeking from this RandomAccessBgzFile will
     * change the stream's position either.</p>
     * 
     * <p>Note: <code>in</code> won't be closed when calling {@link #close()} method.</p>
     * 
     * @param in
     * @throws IOException If IO error occurs while building index
     * @throws NullPointerException If <code>in</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     */
    public RandomAccessBgzFile(FileInputStream in) 
            throws IOException, MalformedBgzipDataException {
        this(in.getChannel(), false);
    }
    
    /**
     * <p>Constructs a RandomAccessBgzFile instance using existing {@link SeekableByteChannel}.</p>
     * 
     * <p>Note: All following read/seek operations will starts from <code>channel</code>'s 
     * current position. Reading or seeking this RandomAccessBgzFile will affect <code>channel</code>'s 
     * position, and vice versa.</p>
     * 
     * <p>Note: <code>channel</code> won't be closed when calling {@link #close()} method.</p>
     * 
     * @param channel
     * @throws IOException If IO error occurs while building index
     * @throws NullPointerException If <code>channel</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     */
    public RandomAccessBgzFile(SeekableByteChannel channel) 
            throws IOException, MalformedBgzipDataException {
        this(channel, false);
    }
    
    private RandomAccessBgzFile(SeekableByteChannel channel, boolean closeChannelOnClose) 
            throws IOException, MalformedBgzipDataException {
        if (channel == null) {
            throw new NullPointerException();
        }
        this.channel = channel;
        this.basePosition = channel.position();
        
        // Build index
        NavigableMap<Long, BgzipBlock> blockTree = new TreeMap<>();
        long inputOffset = 0;
        while (true) {
            BgzipBlock block = readBlock();
            if (block != null) {
                blockTree.put(inputOffset, block);
                inputOffset += block.getInputLength();
            } else {
                break;
            }
        }
        
        this.blockTree = blockTree;
        this.inputLength = inputOffset;
        this.closeChannelOnClose = closeChannelOnClose;
    }
    
    /**
     * <p>Close this {@link RandomAccessBgzFile}</p>
     * 
     * <p>Once this method is called, the behavior of the {@link RandomAccessBgzFile} 
     * object is undefined.</p>
     * 
     * <p>Make sure this method is called when finish using {@link RandomAccessBgzFile}
     * object. Otherwise off-heap memory leak may occur.</p>
     * 
     * <p>Note: This method won't close underlying stream / random access file.</p>
     * 
     * @throws IOException If I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (closeChannelOnClose) {
            channel.close();
        }
        inflater.end();
    }
    
    /**
     * <p>Sets this {@link RandomAccessBgzFile}'s file position, relative to uncompressed data</p>
     * 
     * @param pos
     * @throws IOException If pos is negative or out of uncompressed data's length
     * @see {@link #inputLength()}
     */
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > inputLength) {
            throw new IOException(String.format("Position %d out of range", pos));
        }
        this.pos = pos;
    }
    
    /**
     * <p>Gets this {@link RandomAccessBgzFile}'s file position, 
     * relative to uncompressed data</p>
     * 
     * @return This {@link RandomAccessBgzFile}'s file position
     */
    public long getPosition() {
        return pos;
    }
    
    /**
     * <p>Attempts to skip over n bytes of input discarding the skipped bytes, 
     * relative to uncompressed data.</p>
     * 
     * <p>This method may skip over some smaller number of bytes, possibly zero. 
     * This may result from any of a number of conditions; reaching end of file 
     * before n bytes have been skipped is only one possibility. This method 
     * never throws an {@link EOFException}. The actual number of bytes skipped is returned. 
     * If n is negative, no bytes are skipped.</p>
     * 
     * @param n
     * @return The actual number of bytes skipped
     * @throws IOException If I/O error occurs
     */
    public int skipBytes(int n) throws IOException {
        if (n > 0) {
            long newPos = pos + n > inputLength ? inputLength : pos + n;
            long actual = newPos - pos;
            pos = newPos;
            return (int) actual;
        } else {
            return 0;
        }
    }
    
    /**
     * <p>Gets uncompressed data length</p>
     * 
     * @return Total uncompressed data length
     */
    public long inputLength() {
        return inputLength;
    }
    
    /**
     * <p>Read up to <code>b.length</code> bytes of uncompressed data from this {@link RandomAccessBgzFile} 
     * into <code>b</code></p>
     * 
     * <p>This method will block until at least one byte of input is available.</p>
     * 
     * @param b The buffer into which the data will read
     * @return The total number of bytes read into the buffer, 
     *         or -1 if there is no more data because the end of this file has been reached. 
     * @throws IOException If I/O error occurs
     * @throws NullPointerException If <code>b</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
    
    /**
     * <p>Read up to <code>len</code> bytes of uncompressed data from this {@link RandomAccessBgzFile} 
     * into <code>b</code> starting from <code>off</code></p>
     * 
     * <p>This method will block until at least one byte of input is available.</p>
     * 
     * @param b The buffer into which the data will read
     * @param off The start offset in array b at which the data is written.
     * @param len The maximum number of bytes read
     * @return The total number of bytes read into the buffer, 
     *         or -1 if there is no more data because the end of this file has been reached. 
     * @throws IOException If I/O error occurs
     * @throws NullPointerException If <code>b</code> is <code>null</code>
     * @throws MalformedBgzipDataException If compressed data is not valid BGZF format
     */
    public int read(byte[] b, int off, int len) throws IOException, MalformedBgzipDataException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (pos >= inputLength) {
            return -1;
        }
        
        int cb = 0;
        
        // try read from preceding/following cache
        LocalCache[] caches = new LocalCache[] { preceding, following };
        for (LocalCache cache : caches) {
            if (cache != null && pos >= cache.pos && (pos - cache.pos) <= Integer.MAX_VALUE) {
                int bytesAvailableInCache = (int) (cache.pos + cache.data.length - pos);
                if (bytesAvailableInCache > 0) {
                    int copyLength = Math.min(bytesAvailableInCache, len);
                    System.arraycopy(cache.data, (int) (pos - cache.pos), b, off, copyLength);
                    cb += copyLength;
                    off += copyLength;
                    len -= copyLength;
                    pos += copyLength;
                    if (len == 0) {
                        return cb;
                    }
                }
            }
        }
        
        // find blocks
        List<Entry<Long, BgzipBlock>> blocks = new ArrayList<>();
        if (!blockTree.containsKey(pos)) {
            blocks.add(blockTree.floorEntry(pos));
        }
        blocks.addAll(blockTree.tailMap(pos).headMap(pos + len).entrySet());
        
        for (int i = 0; i < blocks.size(); i++) {
            Entry<Long, BgzipBlock> entry = blocks.get(i);
            BgzipBlock block = entry.getValue();
            long inputOffset = entry.getKey();
            int inputLength = block.getInputLength();
            
            // Read compresed data from block
            byte[] compressed = new byte[block.getDataLength()];
            channel.position(basePosition + block.getDataOffset());
            int n = channel.read(ByteBuffer.wrap(compressed));
            if (n != block.getDataLength()) {
                throw new MalformedBgzipDataException("Wrong compressed data block: block data incomplete to read");
            }
            
            // Uncompress
            inflater.setInput(compressed);
            try {
                byte[] inputData = new byte[inputLength];
                int ret = inflater.inflate(inputData);
                if (ret == 0) {
                    throw new MalformedBgzipDataException("Wrong compressed data block: block data incomplete to decompress");
                } else if (ret != inputLength) {
                    throw new MalformedBgzipDataException("Wrong compressed data block: not fully uncompressed");
                }
                
                if (i == 0) {
                    preceding = new LocalCache(inputOffset, inputData);
                }
                if (i == blocks.size() - 1) {
                    following = new LocalCache(inputOffset, inputData);
                }
                
                long copyStart = 0;
                int copyLength = inputLength;
                if (inputOffset < pos) {
                    long copySkip = pos - inputOffset;
                    copyStart += copySkip;
                    copyLength -= copySkip;
                }
                if (copyLength > len) {
                    copyLength = len;
                }
                System.arraycopy(inputData, (int) copyStart, b, off, copyLength);
                
                len -= copyLength;
                pos += copyLength;
                off += copyLength;
                cb += copyLength;
            } catch (DataFormatException e) {
                throw new MalformedBgzipDataException("Wrong compressed data block: invalid zlib format", e);
            } finally {
                inflater.reset();
            }
        }
        
        return cb;
    }
    
    private BgzipBlock readBlock() throws IOException {
        BgzipBlock.Builder builder = BgzipBlock.builder();
        
        ByteBuffer bb;
        
        // Headers
        bb = ByteBuffer.allocateDirect(12);
        int headerBytes = channel.read(bb);
        ((Buffer) bb).rewind();
        if (headerBytes != 12) {
            throw new MalformedBgzipDataException("Header is broken");
        }
        
        // ID1 = 0x1f
        // ID2 = 0x8b
        // CM = 0x08
        // FLG = 0x04
        final int f1 = bb.getInt();
        if (f1 != 0x1f8b0804) {
            throw new MalformedBgzipDataException("Malformed block header");
        }
        
        // sizeof(MTIME) = 4
        // sizeof(XFL) = 1
        // sizeof(OS) = 1
        bb.position(10);
        
        // XLEN
        final int xlen = BgzipUtils.readUint16(bb);
        
        // Extra Subfield
        bb = ByteBuffer.allocateDirect(xlen);
        channel.read(bb);
        ((Buffer) bb).rewind();
        
        int bSize = -1;
        int remainXlen = xlen;
        while (remainXlen > 0) {
            // SI1: uint8 
            // SI2: uint8
            int si = bb.getShort();
            // SLEN: uint16
            int slen = BgzipUtils.readUint16(bb);
            
            remainXlen -= 4 + slen;
            if (remainXlen < 0) {
                throw new MalformedBgzipDataException("Bad extra field block");
            }
            
            if (si == 0x4243) {
                // BGZF subfield
                if (slen != 2) {
                    throw new MalformedBgzipDataException("Bad subfield length");
                }
                // BSIZE
                if (bSize == -1) {
                    bSize = BgzipUtils.readUint16(bb);
                    if (bSize < 0) {
                        throw new MalformedBgzipDataException("Bad BSIZE field");
                    }
                    builder.blockSize(bSize + 1);
                } else {
                    // already set bsize, duplicate BGZF subfield
                    throw new MalformedBgzipDataException("Duplicate BGZF extrac subfield detected");
                }
            } else {
                bb.position(bb.position() + slen);
            }
        }
        
        if (bSize < 0) {
            throw new MalformedBgzipDataException("Not a BGZF file");
        }
        
        // Compressed data + CRC
        final int dataLength = bSize - xlen - 19;
        if (dataLength < 0) {
            throw new MalformedBgzipDataException("Bad data length");
        }
        builder.dataLength(dataLength);
        
        long dataOffset = channel.position() - basePosition;
        builder.dataOffset(dataOffset);
        // Skip data block
        channel.position(basePosition + dataOffset + dataLength + 4);  // sizeof(CRC32) = 4
        
        // ISIZE
        bb = ByteBuffer.allocateDirect(4);
        channel.read(bb);
        ((Buffer) bb).rewind();
        
        final int isize = (int) BgzipUtils.readUint32(bb);  // ISIZE of BGZF is limited to 0 ~ 65536
        builder.inputLength(isize);
        
        // Detect EOF marker
        if (isize == 0) {
            // null means EOF
            return null;
        }
        
        return builder.build();
    }
    
    private static class LocalCache {
        final long pos;
        final byte[] data;
        LocalCache(long pos, byte[] data) {
            super();
            this.pos = pos;
            this.data = data;
        }
    }
    
}
