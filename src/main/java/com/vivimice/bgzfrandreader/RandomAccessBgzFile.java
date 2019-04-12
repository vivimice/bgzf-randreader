package com.vivimice.bgzfrandreader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class RandomAccessBgzFile implements AutoCloseable, Cloneable {

    private final FileChannel channel;
    private final NavigableMap<Long, BgzipBlock> blockTree;
    private final long inputLength;
    private final Inflater inflater = new Inflater(true);
    
    private long pos = 0;
    
    public RandomAccessBgzFile(File file) throws IOException {
        this(new RandomAccessFile(file, "r"));
    }
    
    private RandomAccessBgzFile(
            FileChannel channel,
            NavigableMap<Long, BgzipBlock> blockTree,
            long inputLength) {
        this.channel = channel;
        this.blockTree = blockTree;
        this.inputLength = inputLength;
    }
    
    public RandomAccessBgzFile(RandomAccessFile file) throws IOException {
        if (file == null) {
            throw new NullPointerException();
        }
        this.channel = file.getChannel();
        
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
        
        this.blockTree = Collections.unmodifiableNavigableMap(blockTree);
        this.inputLength = inputOffset;
    }
    
    @Override
    public RandomAccessBgzFile clone() {
        return new RandomAccessBgzFile(channel, blockTree, inputLength);
    }
    
    @Override
    public void close() throws IOException {
        channel.close();
        inflater.end();
    }
    
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > inputLength) {
            throw new IOException(String.format("Position %d out of range", pos));
        }
        this.pos = pos;
    }
    
    public long getPosition() {
        return pos;
    }
    
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
    
    public long inputLength() {
        return inputLength;
    }
    
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

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
        
        // find blocks
        List<Entry<Long, BgzipBlock>> blocks = new ArrayList<>();
        if (!blockTree.containsKey(pos)) {
            blocks.add(blockTree.floorEntry(pos));
        }
        blocks.addAll(blockTree.tailMap(pos).headMap(pos + len).entrySet());
        
        int cb = 0;
        for (Entry<Long, BgzipBlock> entry : blocks) {
            BgzipBlock block = entry.getValue();
            long inputOffset = entry.getKey();
            int inputLength = block.getInputLength();
            
            // Read compresed data from block
            byte[] compressed = new byte[block.getDataLength()];
            channel.position(block.getDataOffset());
            int n = channel.read(ByteBuffer.wrap(compressed));
            if (n != block.getDataLength()) {
                throw new MalformedBgzipDataException("Wrong compressed data block: block data incomplete to read");
            }
            
            // Uncompress
            inflater.setInput(compressed);
            try {
                // skip if neccessary
                if (inputOffset < pos) {
                    int skip = (int) (pos - inputOffset);
                    inflater.inflate(new byte[skip]);
                    inputLength -= skip;
                }
                
                int length = (len > inputLength) ? inputLength : len;
                int ret = inflater.inflate(b, off, length);
                if (ret == 0) {
                    throw new MalformedBgzipDataException("Wrong compressed data block: block data incomplete to decompress");
                } else if (ret != length) {
                    throw new MalformedBgzipDataException("Wrong compressed data block: not fully uncompressed");
                }
                
                len -= length;
                pos += length;
                off += length;
                cb += length;
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
        bb.rewind();
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
        bb.rewind();
        
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
        
        long dataOffset = channel.position();
        builder.dataOffset(dataOffset);
        // Skip data block
        channel.position(dataOffset + dataLength + 4);  // sizeof(CRC32) = 4
        
        // ISIZE
        bb = ByteBuffer.allocateDirect(4);
        channel.read(bb);
        bb.rewind();
        
        final int isize = (int) BgzipUtils.readUint32(bb);  // ISIZE of BGZF is limited to 0 ~ 65536
        builder.inputLength(isize);
        
        // Detect EOF marker
        if (isize == 0) {
            // null means EOF
            return null;
        }
        
        return builder.build();
    }
    
}
