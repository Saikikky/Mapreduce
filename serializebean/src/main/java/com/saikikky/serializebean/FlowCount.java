package com.saikikky.serializebean;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount {
	/**
	 * 接收日志的一行数据，key为行的偏移量，value为此行数据
	 * 输出时，以手机号为key，value应为一个上行流量，下行流量和整体流量的整体
	 * FlowBean实现可序列化
	 * @author saikikky
	 *
	 */
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) 
							throws IOException, InterruptedException {
			
			// 将一行内容转成string
			String line = value.toString();
			// 切分字段
			String[] fields = line.split("\t");
			// 取出手机号
			String phoneNbr = fields[0];
			// 取出上行流量和下行流量
			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
		}
	}
	
	/**
	 * reduce接收一个手机号标识的key，以及这个手机号对应的bean对象集合
	 * 迭代bean对象集合，累加各项，形成一个新的bean对象
	 * @author saikikky
	 *
	 */
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
							throws IOException, InterruptedException {
			long sum_upFlow = 0;
			long sum_dFlow = 0;
			
			// 遍历所有bean， 将其中的上行流量，下行流量分别累加
			for (FlowBean bean : values) {
				sum_upFlow += bean.getUpFlow();
				sum_dFlow += bean.getdFlow(); 
			}
			
			FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
			context.write(key, resultBean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		// 指定自定义的数据分区器
		job.setPartitionerClass(ProvincePartitioner.class);
		// 同时指定相应“分区”数量的reducetask
		job.setNumReduceTasks(5);
		
		// mapper输入数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		// 最终输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 将job中配置的相关参数，以及job所用的java类所在jar包，提交给yarn运行
		// job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
	

}
