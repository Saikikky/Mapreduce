package com.saikikky.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper 每读到一行数据，就会调用一次这个map方法
 * map的处理流程是接收一个key value对，然后对业务逻辑处理，最后输出一个key value对
 * @author saikikky
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// key值默认为起始偏移量 value为读到的一行的数据内容
	// 输出key是word 输出value是数量
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		// 得到输入的每一行数据
		String line = value.toString();
		
		// 通过空格分割
		String[] words = line.split(" ");
		
		// 循环遍历 输出
		for(String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
