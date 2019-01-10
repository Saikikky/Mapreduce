package com.saikikky.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountMapReduce {

	public static void main(String[] args) throws Exception {
		// 创建配置文件
		Configuration conf = new Configuration();
		
		// 创建job对象
		Job job = Job.getInstance(conf, "wordcount");
		
		// 设置运行job的类
		job.setJarByClass(WordcountMapReduce.class);
		
		// 设置mapper类
		job.setMapperClass(WordcountMapper.class);
		
		// 设置reduce类
		job.setReducerClass(WordcountReducer.class);
		
		// 设置map输出的key value
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 设置输入输出的路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://127.0.0.1:9000/usr/saikikky/input.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/usr/saikikky/output"));
		
		// 提交job
		boolean b = job.waitForCompletion(true);
		
		if (!b) {
			System.out.println("wordcount task fail!");
		}
	}
}
