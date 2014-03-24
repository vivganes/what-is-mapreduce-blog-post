package com.vivekGanesan;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatchMakerReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context output)
			throws IOException, InterruptedException {
		int count = 0;
		for(LongWritable value: values){
			count+= value.get();
		}
		output.write(key, new LongWritable(count));
	}
}