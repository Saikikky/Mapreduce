package com.saikikky.serializebean;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器Partitioner
 * 根据手机号判断属于哪个分区，有几个分区就有几个reducetask，每个reducetask输出一个文件，不同分区中的数据就写入了不同的结果文件中。
 * @author saikikky
 *
 */

public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();

	static {
		provinceDict.put("137", 0);
		provinceDict.put("133", 1);
		provinceDict.put("138", 2);
		provinceDict.put("135", 3);
	}
	
	
	// getPartition取得手机号的前缀，去所谓的数据库中获取区号，如果没有就用4(其他分区)表示
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		String prefix = key.toString().substring(0, 3);
		Integer provinceId = provinceDict.get(prefix);
		
		return provinceId == null ? 4 : provinceId;
	}
}
