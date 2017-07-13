package team.zzyc.task2.alternative;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class StatisticsPartitioner extends HashPartitioner<Text,  IntWritable>{

	public int getPartition(Text key,  IntWritable value, int numReduceTasks){
		
		String ip=key.toString().split("@")[0];
		
		return super.getPartition(new Text(ip), value, numReduceTasks);
	}
	
}
