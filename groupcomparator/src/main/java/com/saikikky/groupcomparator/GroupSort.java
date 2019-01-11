package com.saikikky.groupcomparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupSort {

	static class SortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		OrderBean bean = new OrderBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields = line.split(",");
			bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));
			context.write(bean, NullWritable.get());
		}
	}
	
	// Reduce只需要输出第一条数据即可
	static class SortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
		
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> val, Context context)
							throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		// 创建任务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GroupSort.class);
		
		// 任务输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setPartitionerClass(ItemIdPartitioner.class);
		job.setNumReduceTasks(2);
		
		// 输入文件路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 提交任务
		job.waitForCompletion(true);
	}
}

