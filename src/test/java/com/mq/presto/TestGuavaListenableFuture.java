package com.mq.presto;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.objects.Object2BooleanArrayMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestGuavaListenableFuture
{
    private  ExecutorService service= Executors.newFixedThreadPool(2);
    @Test
    public void testWithFuture()
            throws ExecutionException, InterruptedException
    {
        Future<Integer> future=service.submit(()->{
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 10;
        });

        //Future问题在于这里会堵塞，直到获取到结果
        Object result=future.get();

        System.out.println("result is "+result);

        System.out.println("==============");
    }

    @Test
    public  void testWithListenableFuture()
    {
        ListeningExecutorService listeningExecutorService= MoreExecutors.listeningDecorator(service);

        ListenableFuture<Integer> listenableFuture=listeningExecutorService.submit(()->{
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 10;
        });

        SettableFuture<Integer> settableFuture=SettableFuture.create();
        settableFuture.set(100);

        Futures.addCallback(settableFuture,new MyCallBack(),service);
//        listenableFuture.addListener(()->System.out.println("I am finished"),service);
//        Futures.addCallback(listenableFuture,new MyCallBack(),service);
        System.out.println("============");
    }

    static class MyCallBack implements FutureCallback<Integer>
    {
        @Override
        public void onSuccess(@Nullable Integer integer)
        {
            System.out.println("I am finished and the result is "+integer);
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            throwable.printStackTrace();
        }
    }

    @Test
    public  void testWithCompletableFuture()
    {
        CompletableFuture<Integer> future=CompletableFuture.supplyAsync(()->
        {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 10;
        },service);

        future.whenComplete((v,t)->System.out.println("I am finished and the result is "+v));
        System.out.println("=============");
    }


}
