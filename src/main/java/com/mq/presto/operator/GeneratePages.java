package com.mq.presto.operator;

import com.mq.presto.source.Block;
import com.mq.presto.source.IntArrayBlock;
import com.mq.presto.source.LongArrayBlock;
import com.mq.presto.source.Page;

import java.util.Arrays;
import java.util.Random;

import static java.util.Optional.of;

public class GeneratePages
{
    private static int count=0;
    public static Page generateSourceData(int positionCount)
    {
        Random random=new Random();

        boolean[] valueIsNull=new boolean[positionCount];
        Arrays.fill(valueIsNull,false);

        long[] array1=new long[positionCount];
        long[] array2=new long[positionCount];

        for(int i=0;i<positionCount;i++)
        {
            array1[i]=random.nextInt(1000);
            array2[i]=i;

        }
        Block block1=new LongArrayBlock(positionCount, of(valueIsNull),array1);
        Block block2=new LongArrayBlock(positionCount, of(valueIsNull),array2);

        Page page=new Page(positionCount,block1,block2);

//        System.out.println("预先生成的page"+count++);
//        System.out.println(page.toString());
        return page;
    }

    public static Page genereateFixedRightData(int num)
    {
        int positionCount=10;
        Random random=new Random();

        boolean[] valueIsNull=new boolean[positionCount];
        Arrays.fill(valueIsNull,false);

        long[] array1=new long[positionCount];
        long[] array2=new long[positionCount];

        for(int i=0;i<positionCount;i++)
        {
            array1[i]=i+num;
            array2[i]=i+num;

        }
        Block block1=new LongArrayBlock(positionCount, of(valueIsNull),array1);
        Block block2=new LongArrayBlock(positionCount, of(valueIsNull),array2);

        Page page=new Page(positionCount,block1,block2);

//        System.out.println("预先生成的page"+count++);
//        System.out.println(page.toString());
        return page;
    }

    public static Page generateFixedLeftData()
    {
        int positionCount=1000;
        Random random=new Random();

        boolean[] valueIsNull=new boolean[positionCount];
        Arrays.fill(valueIsNull,false);

        long[] array1=new long[positionCount];
        long[] array2=new long[positionCount];

        for(int i=0;i<positionCount;i++)
        {
            array1[i]=i;
            array2[i]=random.nextInt(100);

        }
        Block block1=new LongArrayBlock(positionCount, of(valueIsNull),array1);
        Block block2=new LongArrayBlock(positionCount, of(valueIsNull),array2);

        Page page=new Page(positionCount,block1,block2);

//        System.out.println("预先生成的page"+count++);
//        System.out.println(page.toString());
        return page;
    }
}
