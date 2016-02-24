package org.viva.india.programs.learn.mr.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable intWritable : values) {
			maxValue = Math.max(maxValue, intWritable.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}
