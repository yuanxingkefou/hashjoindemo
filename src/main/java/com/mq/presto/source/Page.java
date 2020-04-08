package com.mq.presto.source;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Page
{
    private final Block[] blocks;
    private final int positionCount;
    private final AtomicLong sizeInBytes = new AtomicLong(-1);
    private final AtomicLong retainedSizeInBytes = new AtomicLong(-1);
    private final AtomicLong logicalSizeInBytes = new AtomicLong(-1);

    public Page(Block... blocks)
    {
        this(determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes.get();
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (Block block : blocks) {
                sizeInBytes += block.getSizeInBytes();
            }
            this.sizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

//    public long getLogicalSizeInBytes()
//    {
//        long size = logicalSizeInBytes.get();
//        if (size < 0) {
//            size = 0;
//            for (Block block : blocks) {
//                size += block.getLogicalSizeInBytes();
//            }
//            logicalSizeInBytes.set(size);
//        }
//        return size;
//    }

//    public long getRetainedSizeInBytes()
//    {
//        if (retainedSizeInBytes.get() < 0) {
//            updateRetainedSize();
//        }
//        return retainedSizeInBytes.get();
//    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    /**
     * Gets the values at the specified position as a single element page.  The method creates independent
     * copy of the data.
     */
    public Page getSingleValuePage(int position)
    {
        Block[] singleValueBlocks = new Block[this.blocks.length];
        for (int i = 0; i < this.blocks.length; i++) {
            singleValueBlocks[i] = this.blocks[i].getSingleValueBlock(position);
        }
        return new Page(1, singleValueBlocks);
    }

    public Page getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in page with %s positions", positionOffset, length, positionCount));
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return new Page(length, slicedBlocks);
    }

    public Page appendColumn(Block block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return new Page(newBlocks);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));

        showPage(this);

        return builder.toString();
    }

    private void showPage(Page page)
    {
        for(int i=0;i<page.getChannelCount();i++)
        {
            System.out.print("Block ");
        }
        System.out.println();

        for (int i=0;i<page.positionCount;i++)
        {
            System.out.print("Position"+i+": ");
            for (int j=0;j<page.getChannelCount();j++)
            {
                Block block=page.getBlock(j);
                System.out.print(block.getLong(i)+" ");
            }
            System.out.println();
        }
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

//    public Page getPositions(int[] retainedPositions, int offset, int length)
//    {
//        requireNonNull(retainedPositions, "retainedPositions is null");
//
//        Block[] blocks = new Block[this.blocks.length];
//        Arrays.setAll(blocks, i -> this.blocks[i].getPositions(retainedPositions, offset, length));
//        return new Page(length, blocks);
//    }

    public Page prependColumn(Block column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(String.format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }

        Block[] result = new Block[blocks.length + 1];
        result[0] = column;
        System.arraycopy(blocks, 0, result, 1, blocks.length);

        return new Page(positionCount, result);
    }

//    private void updateRetainedSize()
//    {
//        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(blocks);
//        for (Block block : blocks) {
//            retainedSizeInBytes += block.getRetainedSizeInBytes();
//        }
//        this.retainedSizeInBytes.set(retainedSizeInBytes);
//    }



}
