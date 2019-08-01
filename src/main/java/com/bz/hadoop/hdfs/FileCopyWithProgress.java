package com.bz.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * 写入hdfs文件 输出进度
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws Exception{
        String localSrc =args[0];
        String dst =args[1];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(dst), conf);
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in,fsDataOutputStream,4096,true);
        String removeSuffix = CompressionCodecFactory.removeSuffix("", "");
    }
}
