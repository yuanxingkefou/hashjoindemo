package com.mq.presto.driver;

import com.google.common.collect.ImmutableList;
import com.mq.presto.operator.OperatorContext;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;

public class DriverContext
{
    private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();
    private final Executor notificationExecutor;

    public DriverContext(Executor notificationExecutor)
    {
        this.notificationExecutor=notificationExecutor;
    }

    public OperatorContext addOperatorContext(int operatorId, String operatorType)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");

//        for (OperatorContext operatorContext : operatorContexts) {
//            checkArgument(operatorId != operatorContext.getOperatorId(), "A context already exists for operatorId %s", operatorId);
//        }

        OperatorContext operatorContext = new OperatorContext(
                operatorId,
                operatorType,
                this,
                notificationExecutor);
        return operatorContext;
    }

    public List<OperatorContext> getOperatorContexts()
    {
        return ImmutableList.copyOf(operatorContexts);
    }


}
