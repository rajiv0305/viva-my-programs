/**
 * 
 */
package org.viva.india.programs.learn.mr.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author samyra02
 *
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		parser.parse(value);
		if (parser.isValidTemperature()) {
			context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemp()));	
		}
		
	}
}
