package com.mq.presto.operator;

import com.mq.presto.driver.DriverContext;

import java.util.concurrent.Executor;

public class OperatorContext
{
    private final int operatorId;
    private final String operatorType;
    private final DriverContext driverContext;
    private final Executor executor;

    public OperatorContext(int operatorId,String operatorType,
            DriverContext driverContext,Executor executor)
    {
        this.operatorId=operatorId;
        this.operatorType=operatorType;
        this.driverContext=driverContext;
        this.executor=executor;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public String getOperatorType()
    {
        return operatorType;
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Executor getExecutor()
    {
        return executor;
    }

}
