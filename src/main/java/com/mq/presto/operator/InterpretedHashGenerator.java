/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mq.presto.operator;


import com.google.common.collect.ImmutableList;
import com.mq.presto.source.Block;
import com.mq.presto.source.IntegerType;
import com.mq.presto.source.Page;

import java.util.List;
import java.util.function.IntFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InterpretedHashGenerator
        implements HashGenerator
{
    public static final long INITIAL_HASH_VALUE = 0;
    private final List<IntegerType> hashChannelTypes;
    private final int[] hashChannels;

    public InterpretedHashGenerator(List<IntegerType> hashChannelTypes, List<Integer> hashChannels)
    {
        this(hashChannelTypes, requireNonNull(hashChannels).stream().mapToInt(i -> i).toArray());
    }

    public InterpretedHashGenerator(List<IntegerType> hashChannelTypes, int[] hashChannels)
    {
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
        this.hashChannelTypes = ImmutableList.copyOf(requireNonNull(hashChannelTypes, "hashChannelTypes is null"));
        checkArgument(hashChannelTypes.size() == hashChannels.length);
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        return hashPosition(position, page::getBlock);
    }

    public long hashPosition(int position, IntFunction<Block> blockProvider)
    {
        long result = INITIAL_HASH_VALUE;
        for (int i = 0; i < hashChannels.length; i++) {
//            Type type = hashChannelTypes.get(i);
            result = getHash(result, IntegerType.hashPosition(blockProvider.apply(hashChannels[i]), position));
        }
        return result;
    }
    private long getHash(long previousHashValue,long value)
    {
        return (31*previousHashValue+value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hashChannelTypes", hashChannelTypes)
                .add("hashChannels", hashChannels)
                .toString();
    }
}
