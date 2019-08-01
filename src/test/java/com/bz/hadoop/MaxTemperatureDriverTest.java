package com.bz.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MaxTemperatureDriverTest {
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        Path input = new Path("input");
        Path output = new Path("output");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] {
                input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        //checkOutput(conf, output);
    }
}
