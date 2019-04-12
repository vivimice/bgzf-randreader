package com.vivimice.bgzfrandreader;

import java.io.Serializable;

public final class BgzipBlock implements Serializable {

    private static final long serialVersionUID = 3728331604559369930L;
    
    private final long dataOffset;
    private final int dataLength;
    private final int inputLength;
    private final int blockSize;
    
    public BgzipBlock(int blockSize, long dataOffset, int dataLength, int inputLength) {
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        this.inputLength = inputLength;
        this.blockSize = blockSize;
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public int getDataLength() {
        return dataLength;
    }

    public int getInputLength() {
        return inputLength;
    }

    public int getBlockSize() {
        return blockSize;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private long dataOffset;
        private int dataLength;
        private int inputLength;
        private int blockSize;

        public Builder dataOffset(long dataOffset) {
            this.dataOffset = dataOffset;
            return this;
        }

        public Builder dataLength(int dataLength) {
            this.dataLength = dataLength;
            return this;
        }

        public Builder inputLength(int inputLength) {
            this.inputLength = inputLength;
            return this;
        }

        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }
        
        public BgzipBlock build() {
            return new BgzipBlock(blockSize, dataOffset, dataLength, inputLength);
        }

    }
    
    
}
