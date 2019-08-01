package com.bz.hadoop;

import com.bz.hadoop.test.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 只有温度格式正确才会收集 返回output
 */
public class MaxTemperatureMalformedCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    enum Temperature {
        MALFORMED
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException,IOException{
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        } else if (parser.isMalFormedTemperature()) {
            //如果数据格式有问题
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }
    }

}
