package com.saikikky.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * 合并小文件
 * @author saikikky
 *
 */
public class ManyToOne {
	static class FileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey;
		
		@Override
		protected void setup(Context context) 
				throws IOException, InterruptedException{
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}
		
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
					throws IOException, InterruptedException{
			context.write(filenameKey, value);
		}
	}
	
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(ManyToOne.class);
			
			job.setInputFormatClass(MyInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(BytesWritable.class);
			job.setMapperClass(FileMapper.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://127.0.0.1:9000/usr/saikikky/files"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/usr/saikikky/onefile"));
			
			job.waitForCompletion(true);
		}
}
