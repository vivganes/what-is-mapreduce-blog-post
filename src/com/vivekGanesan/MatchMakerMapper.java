package com.vivekGanesan;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatchMakerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	
	@Override
	public void map(LongWritable key, Text value, Context output) throws IOException,
			InterruptedException {
		
		//If more than one word is present, split using white space.
		String[] words = value.toString().split(" ");
		//each word contributes to a key-value pair
		output.write(new Text(words[0]), new LongWritable(1));
		output.write(new Text(words[1]), new LongWritable(1));
	}
}