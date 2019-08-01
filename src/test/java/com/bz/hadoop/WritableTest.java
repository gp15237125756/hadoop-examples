package com.bz.hadoop;

import org.apache.hadoop.io.IntWritable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class WritableTest {

    public static void main(String[] args) throws Exception{
        IntWritable writable = new IntWritable(163);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        writable.write(dataOutputStream);
        dataOutputStream.close();
        System.out.println(byteArrayOutputStream.toByteArray().length);
    }
}
