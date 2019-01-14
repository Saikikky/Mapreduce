package com.saikikky.multioutput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 输出到多个文件
 * MultipleOutputs
 * @author saikikky
 *
 */
public class MultipleOutputTest {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			context.write(new Text(fields[0]), value);
		}
	}
	
	static class MyReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) 
						throws IOException, InterruptedException{
			for (Text value : values) {
				multipleOutputs.write(NullWritable.get(), value, key.toString());
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MultipleOutputTest.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
