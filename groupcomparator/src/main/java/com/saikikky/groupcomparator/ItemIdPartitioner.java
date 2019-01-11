package com.saikikky.groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 根据订单号的hashcode分区，可以保证订单号相同的在一个分区，reduce中接收到同一个订单的全部记录
 * 同分区的数据是有序的，用到了bean中的比较方法，可以让订单号相同的记录按照金额从大到小排序
 * @author saikikky
 *
 */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
		// 相同id的订单bean，会去向相同的partition
		// 产生的分区数，跟用户设置的reducetask数量保持一致
		return (bean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

	
	
}
