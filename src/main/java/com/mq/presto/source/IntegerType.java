package com.mq.presto.source;

import static java.lang.Long.rotateLeft;

public class IntegerType
{
    public static long hashPosition(Block block,int position)
    {
        return hash(block.getInt(position));
    }

    public static long hash(int value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }

    public static boolean positionEqualsPosition
            (Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        return equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    public static boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftValue = leftBlock.getInt(leftPosition);
        int rightValue = rightBlock.getInt(rightPosition);
        return leftValue == rightValue;
    }

    public static int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        int leftValue = leftBlock.getInt(leftPosition);
        int rightValue = rightBlock.getInt(rightPosition);
        return Integer.compare(leftValue, rightValue);
    }

    public static void appendTo(Block block,int position,BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position)).closeEntry();
        }
    }

    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Integer.BYTES);
    }

    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Integer.BYTES));
    }

    public static BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null,positionCount);
    }

}
