package com.mq.presto.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.operator.Operator;
import com.mq.presto.source.Page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.mq.presto.operator.Operator.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class Driver
{
    private final DriverContext driverContext;
    private final List<Operator> activeOperators;
    // this is present only for debugging
    @SuppressWarnings("unused")
    private final List<Operator> allOperators;

    private final Map<Operator, ListenableFuture<?>> revokingOperators = new HashMap<>();

    public static Driver createDriver(DriverContext driverContext, List<Operator> operators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(operators, "operators is null");
        Driver driver = new Driver(driverContext, operators);
        driver.initialize();
        return driver;
    }

    @VisibleForTesting
    public static Driver createDriver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(firstOperator, "firstOperator is null");
        requireNonNull(otherOperators, "otherOperators is null");
        ImmutableList<Operator> operators = ImmutableList.<Operator>builder()
                .add(firstOperator)
                .add(otherOperators)
                .build();
        return createDriver(driverContext, operators);
    }

    public Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.allOperators = ImmutableList.copyOf(requireNonNull(operators, "operators is null"));
        checkArgument(allOperators.size() > 1, "At least two operators are required");
        this.activeOperators = new ArrayList<>(operators);
        checkArgument(!operators.isEmpty(), "There must be at least one operator");
    }

    // the memory revocation request listeners are added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
//        activeOperators.stream()
//                .map(Operator::getOperatorContext)
//                .forEach(operatorContext -> operatorContext.setMemoryRevocationRequestListener(() -> driverBlockedFuture.get().set(null)));
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public void processInternal()
            throws Exception
    {

//        handleMemoryRevoke();

        try {

            // If there is only one operator, finish it
            // Some operators (LookupJoinOperator and HashBuildOperator) are broken and requires finish to be called continuously
            // TODO remove the second part of the if statement, when these operators are fixed
            // Note: finish should not be called on the natural source of the pipeline as this could cause the task to finish early
            if (!activeOperators.isEmpty() && activeOperators.size() != allOperators.size()) {
                Operator rootOperator = activeOperators.get(0);
                rootOperator.finish();
//                rootOperator.getOperatorContext().recordFinish(operationTimer);
            }

            boolean movedPage = false;
            for (int i = 0; i < activeOperators.size() - 1; i++) {
                Operator current = activeOperators.get(i);
                Operator next = activeOperators.get(i + 1);

                // skip blocked operator
                if (getBlockedFuture(current).isPresent()) {
                    continue;
                }

                // if the current operator is not finished and next operator isn't blocked and needs input...
                if (!current.isFinished() && !getBlockedFuture(next).isPresent() && next.needsInput()) {
                    // get an output page from current operator
                    Page page = current.getOutput();
//                    current.getOperatorContext().recordGetOutput(operationTimer, page);

                    // if we got an output page, add it to the next operator
                    if (page != null && page.getPositionCount() != 0) {
                        next.addInput(page);
//                        next.getOperatorContext().recordAddInput(operationTimer, page);
                        movedPage = true;
                    }

//                    if (current instanceof SourceOperator) {
//                        movedPage = true;
//                    }
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.finish();
//                    next.getOperatorContext().recordFinish(operationTimer);
                }
            }

            for (int index = activeOperators.size() - 1; index >= 0; index--) {
                if (activeOperators.get(index).isFinished()) {
                    // close and remove this operator and all source operators
                    List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                    Throwable throwable = closeAndDestroyOperators(finishedOperators);
                    finishedOperators.clear();
                    if (throwable != null) {
                        throwIfUnchecked(throwable);
                        throw new RuntimeException(throwable);
                    }
                    // Finish the next operator, which is now the first operator.
                    if (!activeOperators.isEmpty()) {
                        Operator newRootOperator = activeOperators.get(0);
                        newRootOperator.finish();
//                        newRootOperator.getOperatorContext().recordFinish(operationTimer);
                    }
                    break;
                }
            }

            // if we did not move any pages, check if we are blocked
//            if (!movedPage) {
//                List<Operator> blockedOperators = new ArrayList<>();
//                List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
//                for (Operator operator : activeOperators) {
//                    Optional<ListenableFuture<?>> blocked = getBlockedFuture(operator);
//                    if (blocked.isPresent()) {
//                        blockedOperators.add(operator);
//                        blockedFutures.add(blocked.get());
//                    }
//                }
//
//                if (!blockedFutures.isEmpty()) {
//                    // unblock when the first future is complete
//                    ListenableFuture<?> blocked = firstFinishedFuture(blockedFutures);
//                    // driver records serial blocked time
//                    driverContext.recordBlocked(blocked);
//                    // each blocked operator is responsible for blocking the execution
//                    // until one of the operators can continue
//                    for (Operator operator : blockedOperators) {
//                        operator.getOperatorContext().recordBlocked(blocked);
//                    }
//                    return blocked;
//                }
//            }

//            return NOT_BLOCKED;
        }
        catch (Throwable t) {
//            List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
//            if (interrupterStack == null) {
//                driverContext.failed(t);
//                throw t;
            t.printStackTrace();
        }

        // Driver thread was interrupted which should only happen if the task is already finished.
        // If this becomes the actual cause of a failed query there is a bug in the task state machine.
//            Exception exception = new Exception("Interrupted By");
//            exception.setStackTrace(interrupterStack.stream().toArray(StackTraceElement[]::new));
//            PrestoException newException = new PrestoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted", exception);
//            newException.addSuppressed(t);
//            driverContext.failed(newException);
//            throw exception;
    }

    public boolean isFinished()
    {
        return activeOperators.size()==0;
    }
    private Throwable closeAndDestroyOperators(List<Operator> operators)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        Throwable inFlightException = null;
        try {
            for (Operator operator : operators) {
                try {
                    operator.close();
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
//                    inFlightException = addSuppressedException(
//                            inFlightException,
//                            t,
//                            "Error closing operator %s for task %s",
//                            operator.getOperatorContext().getOperatorId(),
//                            driverContext.getTaskId());
                }
                try {
//                    operator.getOperatorContext().destroy();
                }
                catch (Throwable t) {
//                    inFlightException = addSuppressedException(
//                            inFlightException,
//                            t,
//                            "Error freeing all allocated memory for operator %s for task %s",
//                            operator.getOperatorContext().getOperatorId(),
//                            driverContext.getTaskId());
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return inFlightException;
    }

    private Optional<ListenableFuture<?>> getBlockedFuture(Operator operator)
    {
        ListenableFuture<?> blocked = revokingOperators.get(operator);
        if (blocked != null) {
            // We mark operator as blocked regardless of blocked.isDone(), because finishMemoryRevoke has not been called yet.
            return Optional.of(blocked);
        }
        blocked = operator.isBlocked();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
//        blocked = operator.getOperatorContext().isWaitingForMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
//        blocked = operator.getOperatorContext().isWaitingForRevocableMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        return Optional.empty();
    }
}
