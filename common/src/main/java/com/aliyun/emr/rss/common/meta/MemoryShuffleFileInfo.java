package com.aliyun.emr.rss.common.meta;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

public class MemoryShuffleFileInfo extends FileInfo{
    private List<ByteBuf> buffer = new ArrayList<>();
    public MemoryShuffleFileInfo(String filePath, List<Long> chunkOffsets) {
        super(filePath, chunkOffsets);
    }

    public MemoryShuffleFileInfo(String filePath) {
        super(filePath);
    }

    public MemoryShuffleFileInfo(File file) {
        super(file);
    }

    public List<ByteBuf> getBufferList() {
        return buffer;
    }
}
