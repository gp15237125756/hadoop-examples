package com.bz.hadoop;

import com.bz.hadoop.test.MaxTemperatureMapperV1;
import com.bz.hadoop.test.MaxTemperatureMapperV2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MaxTemperatureMapperTest {

    /**
     * PASS
     * 输出output
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
// Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperV1())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }
    /**
     * NOT PASS
     * 不输出output
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void ignoresMissingTemperatureRecordV1() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
// Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperV1())
                .withInput(new LongWritable(0), value)
                .runTest();
    }
    /**
     * PASS
     * 不输出output
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void ignoresMissingTemperatureRecordV2() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
// Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperV2())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    /*@Test
    public void parsesMalformedTemperature() throws IOException,
            InterruptedException {
        Text value = new Text("0335999999433181957042302005+37950+139117SAO +0004" +
// Year ^^^^
                "RJSN V02011359003150070356999999433201957010100005+353");
// Temperature ^^^^^
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMalformedCountMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .runTest();
        Counters.Counter c = counters.findCounter(MaxTemperatureMalformedCountMapper.Temperature.MALFORMED);
        assertThat(c.getValue(), is(1L));
    }*/
}
