package com.mq.presto.operator;

import com.google.common.collect.ImmutableList;
import com.mq.presto.driver.DriverContext;
import com.mq.presto.source.Block;
import com.mq.presto.source.BlockBuilder;
import com.mq.presto.source.IntegerType;
import com.mq.presto.source.LongArrayBlock;
import com.mq.presto.source.Page;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class OperatorFactory
{
    private DriverContext driverContext;
    private LookupSourceFactory lookupSourceFactory;

    public OperatorFactory(DriverContext driverContext)
    {
        this.driverContext=driverContext;
    }

    public OperatorFactory(DriverContext driverContext, LookupSourceFactory lookupSourceFactory)
    {
        this.driverContext = driverContext;
        this.lookupSourceFactory = lookupSourceFactory;
    }

    public Operator createOperator(int operatorId,String operatorType)
    {
        Operator operator=null;
        OperatorContext operatorContext=driverContext.addOperatorContext(operatorId,operatorType);

        switch (operatorType)
        {
            case "LeftValuesOperator":
                operator=createValuesOperator(operatorType,operatorContext);
                break;
            case "RightValuesOperator":
                operator=createValuesOperator(operatorType,operatorContext);
                break;
            case "HashBuilderOperator":
                operator=createHashBuilderOperator(operatorContext);
                break;
            case "LookupJoinOperator":
                operator=createLookupJoinOperator(operatorContext);
                break;
            case "PagesBufferOperator":
                operator=createPagesBufferOperator(operatorContext);
                break;
        }
        return operator;
    }

    private ValuesOperator createValuesOperator(String operatorType, OperatorContext operatorContext)
    {

        if(operatorType.equals("LeftValuesOperator"))
        {
            System.out.println("Probe表生成测试数据：");
            return new ValuesOperator(operatorContext,generatePages(100,true));
        }
        else
        {
            System.out.println("Build表生成测试数据：");
            return new ValuesOperator(operatorContext,generatePages(9,false));
        }
    }

    private HashBuilderOperator createHashBuilderOperator(OperatorContext operatorContext)
    {
       HashBuilderOperator hashBuilderOperator=new HashBuilderOperator(operatorContext, lookupSourceFactory);
       return hashBuilderOperator;
    }

    private LookupJoinOperator createLookupJoinOperator(OperatorContext operatorContext)
    {
        LookupJoinOperator lookupJoinOperator=new LookupJoinOperator(operatorContext, lookupSourceFactory);
        return lookupJoinOperator;
    }

    private PagesBufferOperator createPagesBufferOperator(OperatorContext operatorContext)
    {
        List<Page> pageBuffer=new ArrayList<>();
        PagesBufferOperator pagesBufferOperator=new PagesBufferOperator(operatorContext,pageBuffer);
        return pagesBufferOperator;
    }
    private List<Page> generatePages(int num,boolean left)
    {
        List<Integer> hashChannels=new ArrayList<>();
        hashChannels.add(0);
        List<Page> pages=new ArrayList<>(num);
        int count=0;
        for(int i=0;i<num;i++)
        {
            Page page;
            if(left) {
                page = GeneratePages.generateFixedLeftData();
            }
            else
            {
                page=GeneratePages.genereateFixedRightData(count);
                count+=20;
            }

            pages.add(getHashPage(page,hashChannels));
        }
        return pages;
    }
    private Page getHashPage(Page page,List<Integer> hashChannels)
    {
        Block[] hashBlocks=new Block[hashChannels.size()];

        int hashBlockIndex=0;

        for (int channel : hashChannels)
        {
            hashBlocks[hashBlockIndex++]=page.getBlock(channel);
        }
        return page.appendColumn(getHashBlock(hashBlocks));
    }

    private Block getHashBlock(Block[] hashBlocks)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }

        List<IntegerType> hashTypes=new ArrayList<>();
        hashTypes.add(new IntegerType());

        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = IntegerType.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            builder.writeLong(hashGenerator.hashPosition(i, page));

        }
        return builder.build();
    }
}
