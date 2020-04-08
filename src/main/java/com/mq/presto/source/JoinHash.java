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
package com.mq.presto.source;


import com.mq.presto.operator.InternalJoinFilterFunction;
import com.mq.presto.operator.JoinFilterFunction;
import com.mq.presto.operator.StandardJoinFilterFunction;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class JoinHash

{
    private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(JoinHash.class).instanceSize();
    private final PagesHash pagesHash;

    // we unwrap Optional<JoinFilterFunction> to actual verifier or null in constructor for performance reasons
    // we do quick check for `filterFunction == null` in `isJoinPositionEligible` to avoid calls to applyFilterFunction


    // we unwrap Optional<PositionLinks> to actual position links or null in constructor for performance reasons
    // we do quick check for `positionLinks == null` to avoid calls to positionLinks
    @Nullable
    private final PositionLinks positionLinks;

//    private final JoinFilterFunction filterFunction;


    public JoinHash(PagesHash pagesHash, Optional<PositionLinks> positionLinks)
    {
        this.pagesHash = requireNonNull(pagesHash, "pagesHash is null");
//        this.filterFunction = new StandardJoinFilterFunction(new InternalJoinFilterFunction() {
//            @Override
//            public boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
//            {
//                return false;
//            }
//        },);
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null").orElse(null);
    }


    public boolean isEmpty()
    {
        return getJoinPositionCount() == 0;
    }


    public final int getChannelCount()
    {
        return pagesHash.getChannelCount();
    }


    public long getJoinPositionCount()
    {
        return pagesHash.getPositionCount();
    }


    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + pagesHash.getInMemorySizeInBytes() + (positionLinks == null ? 0 : positionLinks.getSizeInBytes());
    }


    public long joinPositionWithinPartition(long joinPosition)
    {
        return joinPosition;
    }


    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }


    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage, rawHash);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    private long startJoinPosition(int currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (currentJoinPosition == -1) {
            return -1;
        }
        if (positionLinks == null) {
            return currentJoinPosition;
        }
        return positionLinks.start(currentJoinPosition, probePosition, allProbeChannelsPage);
    }


    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (positionLinks == null) {
            return -1;
        }
        return positionLinks.next(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }


//    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
//    {
//        return filterFunction == null || filterFunction.filter(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
//    }

    public void appendTo(long position, PageBuilder pageBuilder,int outputChannelOffset)
    {
        pagesHash.appendTo(toIntExact(position), pageBuilder,outputChannelOffset);
    }


    public void close()
    {
    }

    public PagesHash getPagesHash()
    {
        return this.pagesHash;
    }
}
