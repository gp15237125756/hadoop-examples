package com.bz.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WritableComparatorTest {

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static void main(String[] args) throws Exception{
        WritableComparator comparator = WritableComparator.get(IntWritable.class);
        IntWritable w1 = new IntWritable(163);
        IntWritable w2 = new IntWritable(67);
        int result = comparator.compare(w1, w2);
        System.out.println(result);
        byte[] b1 = serialize(w1);
        byte[] b2 = serialize(w2);
        int result2 = comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
        System.out.println(org.apache.hadoop.util.StringUtils.byteToHexString(b1));
        System.out.println(result2);
        byte[] data = serialize(new VIntWritable(163));
        System.out.println(org.apache.hadoop.util.StringUtils.byteToHexString(data));


    }
}
