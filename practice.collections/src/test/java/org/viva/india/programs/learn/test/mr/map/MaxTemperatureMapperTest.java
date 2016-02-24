package org.viva.india.programs.learn.test.mr.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import org.viva.india.programs.learn.mr.map.MaxTemperatureMapper;

public class MaxTemperatureMapperTest {

	@Test
	public void processValidRecord() throws IOException {

		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"
				+ "99999V0203201N00261220001CN9999999N9-00111+99999999999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0), value)
		.withOutput(new Text("1950"), new IntWritable(-11))
		.runTest();
	}
	
	public void ignoresMissingTemperatureRecord() throws IOException{
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"+
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		new MapDriver<LongWritable,Text,Text,IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0), value)
		.runTest();
		
	}
}
