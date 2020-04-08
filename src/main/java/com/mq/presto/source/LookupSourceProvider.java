package com.mq.presto.source;

public class LookupSourceProvider
{
    private LookupSource lookupSource;

    public LookupSourceProvider()
    {

    }

    public LookupSource getLookupSource()
    {
        return lookupSource;
    }

    public void setLookupSource(LookupSource lookupSource)
    {
        this.lookupSource = lookupSource;
    }

    public boolean isDone()
    {
        if(lookupSource!=null)
            return true;
        else
            return false;
    }

    public void close()
    {
        lookupSource=null;
    }
}
