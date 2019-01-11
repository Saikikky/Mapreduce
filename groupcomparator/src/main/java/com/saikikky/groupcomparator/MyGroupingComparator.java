package com.saikikky.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 由于map结果中的key是bean不是普通数据类型，所以需要使用自定义比较器来分组。
 * 读到分区1中的数据之后，比较订单号，相同放入一组，默认以第一条记录的key作为这组记录的key值
 * 这样做的好处是数据已经从大到小排序，而程序结果要求得到每个订单号的最大值正好符合要求。
 * @author saikikky
 *
 */
public class MyGroupingComparator extends WritableComparator{

	public MyGroupingComparator() {
		super(OrderBean.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean ob1 = (OrderBean)a;
		OrderBean ob2 = (OrderBean)b;
		return ob1.getItemid().compareTo(ob2.getItemid());
	}
}
