package com.saikikky.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reduce
 * map传过来的数据为(good,1) (good,1)
 * reduce接收时成了key：good value：(1,1) 接收的是同一个key的一组value
 * @author saikikky
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
		 
		Integer count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}

	
}
