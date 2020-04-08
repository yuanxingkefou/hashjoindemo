package com.mq.presto;

import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.driver.Driver;
import com.mq.presto.driver.DriverContext;
import com.mq.presto.operator.HashBuilderOperator;
import com.mq.presto.operator.LookupJoinOperator;
import com.mq.presto.operator.LookupSourceFactory;
import com.mq.presto.operator.Operator;
import com.mq.presto.operator.OperatorFactory;
import com.mq.presto.operator.PagesBufferOperator;
import com.mq.presto.operator.ValuesOperator;
import com.mq.presto.source.LookupSourceProvider;
import com.mq.presto.source.Page;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mq.presto.utils.SyntheticAddress.decodePosition;
import static com.mq.presto.utils.SyntheticAddress.decodeSliceIndex;
import static com.mq.presto.utils.SyntheticAddress.encodeSyntheticAddress;

public class TestHashJoinOperator
{
    private DriverContext leftDriverContext = new DriverContext(command -> {

    });

    private DriverContext rightDriverContext = new DriverContext(command -> {

    });

    private List<Operator> leftOperators = new ArrayList<>();
    private List<Operator> rightOperators = new ArrayList<>();

    private Driver leftDriver;
    private Driver rightDriver;

    private ValuesOperator leftValuesOperator;
    private ValuesOperator rightValuesOperator;

    private LookupSourceFactory lookupSourceFactory;


    public void setup()
    {
        lookupSourceFactory = new LookupSourceFactory();

        OperatorFactory leftOperatorFactory = new OperatorFactory(leftDriverContext, lookupSourceFactory);
        OperatorFactory rightOperatorFactory = new OperatorFactory(rightDriverContext, lookupSourceFactory);

        leftValuesOperator = (ValuesOperator) leftOperatorFactory.createOperator(0, "LeftValuesOperator");
        rightValuesOperator = (ValuesOperator) leftOperatorFactory.createOperator(0, "RightValuesOperator");
        HashBuilderOperator hashBuilderOperator = (HashBuilderOperator) rightOperatorFactory.createOperator(1, HashBuilderOperator.class.getSimpleName());
        LookupJoinOperator lookupJoinOperator = (LookupJoinOperator) leftOperatorFactory.createOperator(1, LookupJoinOperator.class.getSimpleName());
        PagesBufferOperator pagesBufferOperator = (PagesBufferOperator) leftOperatorFactory.createOperator(2, PagesBufferOperator.class.getSimpleName());

        leftOperators.add(leftValuesOperator);
        leftOperators.add(lookupJoinOperator);
        leftOperators.add(pagesBufferOperator);
        leftDriver = new Driver(leftDriverContext, leftOperators);

        rightOperators.add(rightValuesOperator);
        rightOperators.add(hashBuilderOperator);
        rightDriver = new Driver(rightDriverContext, rightOperators);
    }

    public static void main(String[] args)
    {
        TestHashJoinOperator test=new TestHashJoinOperator();
        test.setup();
        ExecutorService pool1 = Executors.newFixedThreadPool(1);
        ExecutorService pool2 = Executors.newFixedThreadPool(1);

//        ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProviderFuture();

        AtomicInteger count = new AtomicInteger();
        AtomicInteger num = new AtomicInteger();

        pool2.execute(() -> {
                try {
                    while(!test.leftDriver.isFinished())
                    {
                        num.getAndIncrement();
                        test.leftDriver.processInternal();
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
        });

        pool1.execute(() -> {
                try {
                    while(!test.rightDriver.isFinished())
                    {
                        count.getAndIncrement();
                        test.rightDriver.processInternal();
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
        });
        pool1.shutdown();
        pool2.shutdown();

    }

    @Test
    public void testGenerateDate()
    {
        List<Page> pages = new ArrayList<>();

        Page page = leftValuesOperator.getOutput();

        pages.add(page);
    }

    @Test
    public void testSyntheticAddress()
    {
        LongArrayList valueAddresses = new LongArrayList();
        for (int i = 0; i < 10; i++) {
            long k = encodeSyntheticAddress(0, i);
            valueAddresses.add(k);
        }

        for (int i = 0; i < valueAddresses.size(); i++) {
            int pageIndex = decodeSliceIndex(valueAddresses.get(i));
            int position = decodePosition(valueAddresses.get(i));

            System.out.println(pageIndex + " : " + position);
        }
    }
}
