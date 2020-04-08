package com.mq.presto.operator;

import com.mq.presto.source.Block;
import com.mq.presto.source.Page;

import java.util.ArrayList;
import java.util.List;

public class PagesBufferOperator implements Operator
{
    private OperatorContext operatorContext;

    private final List<Page> pageBuffer;
    private List<Integer> channels;

    private boolean finished;

    public PagesBufferOperator(OperatorContext operatorContext,List<Page> pageBuffer
    )
    {
        this.operatorContext=operatorContext;
        this.pageBuffer=pageBuffer;
        channels=new ArrayList<>();
        channels.add(2);
        channels.add(5);
    }
    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
        showResult();
    }


    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        finished=true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    private void showResult()
    {
        System.out.println("match的page有：");
        for(int i=0;i<pageBuffer.size();i++)
        {
            System.out.println("Page"+i);
            System.out.println(pageBuffer.get(i).toString());
        }
    }
}
