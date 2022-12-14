package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable outV = new IntWritable();

    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;

        //累加
        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        //写出
        context.write(key,outV);
    }

}
