package team.zzyc.task4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

// 按照url分区
public class StatisticsPartitioner extends HashPartitioner<Text, Text>{

	public int getPartition(Text key, Text value, int numReduceTasks){
		
		String url=key.toString().split("@")[0];
		
		return super.getPartition(new Text(url), value, numReduceTasks);
	}
	
}
