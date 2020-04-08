package com.mq.presto.source;

public class LookupSource
{
    private JoinHash joinHash;
    private boolean isSpilled;

    public LookupSource(JoinHash joinHash, boolean isSpilled)
    {
        this.joinHash = joinHash;
        this.isSpilled = isSpilled;
    }

    public JoinHash getJoinHash()
    {
        return joinHash;
    }

    public void setJoinHash(JoinHash joinHash)
    {
        this.joinHash = joinHash;
    }

    public boolean isSpilled()
    {
        return isSpilled;
    }

    public void setSpilled(boolean spilled)
    {
        isSpilled = spilled;
    }
}
