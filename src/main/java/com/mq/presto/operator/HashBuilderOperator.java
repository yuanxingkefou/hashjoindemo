package com.mq.presto.operator;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.source.JoinHash;
import com.mq.presto.source.LookupSource;
import com.mq.presto.source.Page;
import com.mq.presto.source.PagesIndex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.singletonIterator;
import static java.util.Objects.requireNonNull;

public class HashBuilderOperator implements Operator
{
    private State state= State.CONSUMING_INPUT;
    private OperatorContext operatorContext;
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private OptionalInt hashChannel;
    private List<Page> spills=new ArrayList<>();
    private PagesIndex index;
    private LookupSourceFactory lookupSourceFactory;

    public JoinHash joinHash;
    private boolean isSpilled;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();


    public HashBuilderOperator(OperatorContext operatorContext,LookupSourceFactory lookupSourceFactory)
    {
        this.operatorContext=operatorContext;
        //预设好需要进行join匹配条件的列hashChannel和build表需要show的列
        hashChannel= OptionalInt.of(0);
        index=new PagesIndex(hashChannel.getAsInt(),3);

        this.lookupSourceFactory =lookupSourceFactory;
    }

    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return this.operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state)
        {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case CLOSED:
                return NOT_BLOCKED;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT)
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());

//        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
        return stateNeedsInput;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

//        if (lookupSourceFactoryDestroyed.isDone()) {
//            close();
//            return;
//        }

        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);

    }

    private void updateIndex(Page page)
    {
        isSpilled=index.addPage(page);
        if(isSpilled)
        {
            //不需要溢出的先建立hash表进行匹配
            //finish();
            state=State.SPILLING_INPUT;
        }
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
//        checkSuccess(spillInProgress, "spilling failed");
//        spillInProgress = getSpiller().spill(page);
        spill(singletonIterator(page));
    }

    private void spill(Iterator<Page> pageIterator)
    {
        Iterators.addAll(spills,pageIterator);
    }

//    @Override
//    public ListenableFuture<?> startMemoryRevoke()
//    {
////        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");
//
//        if (state == State.CONSUMING_INPUT) {
////            long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
////            index.compact();
////            long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
////            if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
////                finishMemoryRevoke = Optional.of(() -> {});
////                return immediateFuture(null);
////            }
////
////            finishMemoryRevoke = Optional.of(() -> {
////                index.clear();
////                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
////                localRevocableMemoryContext.setBytes(0);
////                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
////                state = State.SPILLING_INPUT;
////            });
//            return spillIndex();
//        }
//        else if (state == State.LOOKUP_SOURCE_BUILT) {
////            finishMemoryRevoke = Optional.of(() -> {
////                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
////                lookupSourceNotNeeded = Optional.empty();
////                index.clear();
////                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
////                localRevocableMemoryContext.setBytes(0);
////                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
////                lookupSourceSupplier = null;
//                state = State.INPUT_SPILLED;
//            });
//            return spillIndex();
//        }
//        else if (operatorContext.getReservedRevocableBytes() == 0) {
//            // Probably stale revoking request
////            finishMemoryRevoke = Optional.of(() -> {});
//            return immediateFuture(null);
//        }
//
//        throw new IllegalStateException(format("State %s can not have revocable memory, but has %s revocable bytes", state, operatorContext.getReservedRevocableBytes()));
//    }

//    private ListenableFuture<?> spillIndex()
//    {
//        checkState(!spiller.isPresent(), "Spiller already created");
//        spiller = Optional.of(singleStreamSpillerFactory.create(
//                index.getTypes(),
//                operatorContext.getSpillContext().newLocalSpillContext(),
//                operatorContext.newLocalSystemMemoryContext(HashBuilderOperator.class.getSimpleName())));
//        return getSpiller().spill(index.getPages());
//    }

//    @Override
//    public void finishMemoryRevoke()
//    {
//        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
//        finishMemoryRevoke.get().run();
//        finishMemoryRevoke = Optional.empty();
//    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
//        if (lookupSourceFactoryDestroyed.isDone()) {
//            close();
//            return;
//        }
//
//        if (finishMemoryRevoke.isPresent()) {
//            return;
//        }
//
        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                spilledStateChange();
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

//            case CLOSED:
//                // no-op
//                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void spilledStateChange()
    {
        checkState(state == State.INPUT_SPILLED);

        state = State.INPUT_UNSPILLING;
    }

    private void finishLookupSourceUnspilling()
    {
        checkState(state == State.INPUT_UNSPILLING);

        Queue<Page> pages = new ArrayDeque<>(spills);

        while (!pages.isEmpty()) {
            Page next = pages.remove();
            index.addPage(next);
            // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
        }
        joinHash =index.createHashTable(hashChannel);
        isSpilled=false;
        LookupSource spilledLookupSource=new LookupSource(joinHash,isSpilled);

//        lookupSourceNotNeeded=Optional.of(lookupSourceFactory.lendLookupSource(spilledLookupSource));
        lookupSourceFactory.setLookupSource(spilledLookupSource);

        state = State.INPUT_UNSPILLED_AND_BUILT;
    }
    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
//        if (lookupSourceFactoryDestroyed.isDone()) {
//            close();
//            return;
//        }

//        JoinHashSupplier partition = index.createJoinHash();
//        if (spillEnabled) {
//            localRevocableMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
//        }
//        else {
//            localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
//        }
//        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        joinHash =index.createHashTable(hashChannel);

        LookupSource lookupSource=new LookupSource(joinHash,isSpilled);

        lookupSourceNotNeeded=Optional.of(lookupSourceFactory.lendLookupSource(lookupSource));
//        lookupSourceFactory.setLookupSource(lookupSource);

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);

        if(!lookupSourceNotNeeded.get().isDone())
            return;

        index.clear();
        state=State.SPILLING_INPUT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
//        if(!lookupSourceNotNeeded.get().isDone())
//            return;
//        index.clear();
        close();
    }
    private void finishSpilledInput()
    {
        checkState(state == State.SPILLING_INPUT);

        if(joinHash==null)
        {
            state=state.CONSUMING_INPUT;
        }
        else {
            state = State.INPUT_SPILLED;
        }
    }
    @Override
    public boolean isFinished()
    {
//        if (lookupSourceFactoryDestroyed.isDone()) {
//            // Finish early when the probe side is empty
//            close();
//            return true;
//        }

        return state == State.CLOSED;
    }

//    private SingleStreamSpiller getSpiller()
//    {
//        return spiller.orElseThrow(() -> new IllegalStateException("Spiller not created"));
//    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally
//
//        lookupSourceSupplier = null;
        state = State.CLOSED;
//        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});
//
//        try (Closer closer = Closer.create()) {
//            closer.register(index::clear);
//            spiller.ifPresent(closer::register);
//            closer.register(() -> localUserMemoryContext.setBytes(0));
//            closer.register(() -> localRevocableMemoryContext.setBytes(0));
//        }
//        catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
}
