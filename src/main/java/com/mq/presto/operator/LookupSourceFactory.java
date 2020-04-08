package com.mq.presto.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.mq.presto.source.JoinHash;
import com.mq.presto.source.LookupSource;
import com.mq.presto.source.LookupSourceProvider;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LookupSourceFactory
{
    private LookupSource lookupSource;
    private LookupSourceProvider lookupSourceProvider;
    private SettableFuture<LookupSourceProvider> lookupSourceFuture;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SettableFuture<?> lookUpSourceNoLongerNeeded = SettableFuture.create();


    public ListenableFuture<LookupSourceProvider> createLookupSourceProviderFuture()
    {
        lookupSourceProvider=new LookupSourceProvider();
        lookupSourceFuture=SettableFuture.create();
//        lookupSourceFuture.set(this.lookupSourceProvider);

        return lookupSourceFuture;
    }

    public ListenableFuture<?> lendLookupSource(LookupSource lookupSource)
    {
        this.lookupSource=lookupSource;
        this.lookupSourceProvider.setLookupSource(lookupSource);
        lookupSourceFuture.set(this.lookupSourceProvider);
        return lookUpSourceNoLongerNeeded;
    }

    public void setLookupSource(LookupSource lookupSource)
    {
        if(lookupSource!=null) {
            this.lookupSource = lookupSource;
            this.lookupSourceProvider.setLookupSource(lookupSource);
            lookupSourceFuture.set(this.lookupSourceProvider);
        }

    }

    public void close()
    {
        this.lookupSource=null;
    }
    public void finishProbeOperator()
    {
        lookUpSourceNoLongerNeeded.set(null);
        lookupSourceFuture.set(null);
    }
}
