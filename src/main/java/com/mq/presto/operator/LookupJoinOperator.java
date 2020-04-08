package com.mq.presto.operator;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.source.Block;
import com.mq.presto.source.DriverYieldSignal;
import com.mq.presto.source.IntegerType;
import com.mq.presto.source.JoinHash;
import com.mq.presto.source.JoinProbe;
import com.mq.presto.source.LookupJoinPageBuilder;
import com.mq.presto.source.LookupSourceProvider;
import com.mq.presto.source.Page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator
{
    private OperatorContext operatorContext;
    private JoinProbe probe;
    private Page outputPage;
    private long joinPosition = -1;

    private final int[] probeOutputChannels = new int[] {0, 1, 2};
    private final List<Integer> probeJoinChannels = new ArrayList<>();
    private final OptionalInt probeHashChannel = OptionalInt.of(2);

    private JoinProbe.JoinProbeFactory joinProbeFactory;
    private LookupSourceFactory lookupSourceFactory;
    private LookupSourceProvider lookupSourceProvider;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderListenableFuture;
    private LookupJoinPageBuilder pageBuilder;
    private boolean finishing;
    private boolean finished;
    private boolean unspilling;
    private boolean closed;
    private Iterator<Page> unspilledInputPagesIterator = emptyIterator();
    private List<Page> unspilledInputPages = new ArrayList<>();

    public LookupJoinOperator(OperatorContext operatorContext, LookupSourceFactory lookupSourceFactory)
    {
        probeJoinChannels.add(0);
        this.operatorContext = operatorContext;
        this.joinProbeFactory = new JoinProbe.JoinProbeFactory(probeOutputChannels, probeJoinChannels, probeHashChannel);
        this.lookupSourceFactory = lookupSourceFactory;
        this.lookupSourceProviderListenableFuture = lookupSourceFactory.createLookupSourceProviderFuture();
        List<IntegerType> buildTypes = new ArrayList<>();
        buildTypes.add(new IntegerType());
        buildTypes.add(new IntegerType());
        buildTypes.add(new IntegerType());
        this.pageBuilder = new LookupJoinPageBuilder(buildTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return this.operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderListenableFuture.isDone()
                && probe == null
                && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        SpillInfo spillInfo = new SpillInfo(lookupSourceProvider.getLookupSource().isSpilled());
        addInput(page, spillInfo);
    }

    private void addInput(Page page, SpillInfo spillInfo)
    {
        if (spillInfo.hasSpilled) {
            collectUnspilledInputPages(page);
//            if (page.getPositionCount()==0)
//                return;
        }

        probe = createJoinProve(page);
    }

    private void collectUnspilledInputPages(Page page)
    {
        unspilledInputPages.add(page);
    }

    public static class SpillInfo
    {
        private final boolean hasSpilled;

        public SpillInfo(boolean hasSpilled)
        {
            this.hasSpilled = hasSpilled;
        }
    }

    private JoinProbe createJoinProve(Page page)
    {
        probe = joinProbeFactory.createJoinProbe(page);
        return probe;
    }

    @Override
    public Page getOutput()

    {
        if (probe == null && !finishing && pageBuilder.isEmpty()) { return null; }

        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderListenableFuture, LookupSourceProvider::close);
            lookupSourceProvider.setLookupSource(null);
        }

        if (probe == null && !finished && unspilling) {
            tryUnspillNext();
        }
        if (probe == null && finishing && !unspilling) {
            lookupSourceFactory.finishProbeOperator();
            lookupSourceProvider=null;
            unspilling = true;
        }
        if (probe != null) {
            processProbe(lookupSourceProvider.getLookupSource().getJoinHash());
        }

        if (outputPage != null) {
            Page output = dropChannel(outputPage);
            outputPage = null;
            return output;
        }
        return null;
    }

    private void tryUnspillNext()
    {
        if(lookupSourceProvider.getLookupSource().getJoinHash().getPagesHash().getPositionCount()==0)
        {
            return;
        }
        if(!unspilledInputPagesIterator.hasNext()&&unspilledInputPages.size()==0)
        {
            finished=true;
            pageBuilder.reset();
            return;
        }
        if (unspilledInputPagesIterator.hasNext()) {
            addInput(unspilledInputPagesIterator.next());
            return;
        }
        else {
            unspilledInputPagesIterator = unspilledInputPages.iterator();
            unspilledInputPages = new ArrayList<>();
        }
    }

    private Page dropChannel(Page page)
    {
        //todo:channels
        List<Integer> channels = new ArrayList<>();
        channels.add(2);
        channels.add(5);

        int channel = 0;
        Block[] blocks = new Block[page.getChannelCount() - channels.size()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            if (channels.contains(i)) {
                continue;
            }
            blocks[channel++] = page.getBlock(i);
        }
        return new Page(blocks);
    }

    private void processProbe(JoinHash joinHash)
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        while (!yieldSignal.isSet()) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(joinHash, yieldSignal)) {
                    break;
                }
//                if (!currentProbePositionProducedRow) {
//                    currentProbePositionProducedRow = true;
//                    if (!outerJoinCurrentPosition()) {
//                        break;
//                    }
//                }
            }
//            currentProbePositionProducedRow = false;
            if (!advanceProbePosition(joinHash)) {
                break;
            }
//            statisticsCounter.recordProbe(joinSourcePositions);
//            joinSourcePositions = 0;
        }
    }

    private boolean tryFetchLookupSourceProvider()

    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderListenableFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderListenableFuture));
//            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    private boolean advanceProbePosition(JoinHash joinHash)
    {
        if (!probe.advanceNextPosition()) {
            clearProbe();
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(joinHash);
        return true;
    }

    private void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        probe = null;
    }

    private void buildPage()
    {
        verify(outputPage == null);
        verify(probe != null);

        if (pageBuilder.isEmpty()) {
            return;
        }

        outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
    }

    private boolean joinCurrentPosition(JoinHash joinHash, DriverYieldSignal yieldSignal)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
//            if (joinHash.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
//                currentProbePositionProducedRow = true;
//
//                pageBuilder.appendRow(probe, joinHash, joinPosition);
//                joinSourcePositions++;
//            }
            //toDo:这里的判断是为了什么？
            pageBuilder.appendRow(probe, joinHash, joinPosition);
            // get next position on lookup side for this probe row
            joinPosition = joinHash.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    private boolean tryBuildPage()
    {
        if (pageBuilder.isFull()) {
            buildPage();
            return true;
        }
        return false;
    }

    @Override
    public void finish()
    {
        if (finishing) { return; }

        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = this.finished && probe == null && pageBuilder.isEmpty() && outputPage == null;

        if (finished) { close(); }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
//        if (!spillInProgress.isDone()) {
//            // Input spilling can happen only after lookupSourceProviderFuture was done.
//            return spillInProgress;
//        }
//        if (unspilledLookupSource.isPresent()) {
//            // Unspilling can happen only after lookupSourceProviderFuture was done.
//            return unspilledLookupSource.get();
//        }

        if (finishing) {
            return NOT_BLOCKED;
        }

        return lookupSourceProviderListenableFuture;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
//            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
//            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
//            spiller.ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
