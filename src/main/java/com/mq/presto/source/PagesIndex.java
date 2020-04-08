package com.mq.presto.source;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.mq.presto.utils.SyntheticAddress.encodeSyntheticAddress;
import static io.airlift.slice.SizeOf.sizeOf;

public class PagesIndex
{
    private int positionCount;
    private int hashChannel;
    private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(PagesIndex.class).instanceSize();
    private long pagesMemorySize;
    private long estimatedSize;
    private final long MemoryLimit=15000;

    private ObjectArrayList<Block>[] channels;
    private LongArrayList valueAddresses;
    private PagesHashStrategyFactory pagesHashStrategyFactory;

    public PagesIndex(int hashChannel,int blocks)
    {
        this.hashChannel=hashChannel;
        //noinspection rawtypes
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[blocks];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        valueAddresses=new LongArrayList();
        estimatedSize = calculateEstimatedSize();
    }

    public boolean addPage(Page page)
    {
        if(page.getPositionCount()==0)
            return false;

        positionCount+=page.getPositionCount();

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);

            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        for(int i=0;i<page.getPositionCount();i++)
        {
            long sliceAddress = encodeSyntheticAddress(pageIndex, i);
            valueAddresses.add(sliceAddress);
        }

        estimatedSize=calculateEstimatedSize();

        if(estimatedSize>MemoryLimit)
            return true;
        else
            return false;
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + addressesArraySize;
    }
    public JoinHash createHashTable(OptionalInt hashChannel)
    {
        PositionLinks.FactoryBuilder positionLinksFactoryBuilder=
                ArrayPositionLinks.builder(valueAddresses.size());

        List<List<Block>> channels = ImmutableList.copyOf(this.channels);


//        PagesHashStrategy hashStrategy=pagesHashStrategyFactory.createPagesHashStrategy(channels,hashChannel);

        List<Integer> outputChannels=new ArrayList<>();
        outputChannels.add(0);
        outputChannels.add(1);
        outputChannels.add(2);

        List<Integer> hashChannels=new ArrayList<>();
        hashChannels.add(this.hashChannel);

        Optional<Integer> sortChannel=Optional.empty();

        PagesHashStrategy hashStrategy=new SimplePagesHashStrategy(outputChannels,channels,hashChannels,sortChannel,false);
        PagesHash pagesHash=new PagesHash(valueAddresses,hashStrategy,positionLinksFactoryBuilder);

        Optional<PositionLinks> positionLinks = Optional.ofNullable(positionLinksFactoryBuilder.build().create());

        JoinHash joinHash=new JoinHash(pagesHash,positionLinks);

        return joinHash;
    }

    public static class PagesHashStrategyFactory
    {
        private final Constructor<? extends PagesHashStrategy> constructor;

        public PagesHashStrategyFactory(Class<? extends PagesHashStrategy> pagesHashStrategyClass)
        {
            try {
                constructor = pagesHashStrategyClass.getConstructor(List.class, OptionalInt.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        public PagesHashStrategy createPagesHashStrategy(List<? extends List<Block>> channels, OptionalInt hashChannel)
        {
            try {
                return constructor.newInstance(channels, hashChannel);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void clear()
    {
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
            channel.trim();
        }
        valueAddresses.clear();
        valueAddresses.trim();
        positionCount = 0;
//        nextBlockToCompact = 0;
        pagesMemorySize = 0;

        estimatedSize = calculateEstimatedSize();
    }
}
