package team.zzyc.task5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class StatisticsPartitioner extends HashPartitioner<Text, Text>{

	public int getPartition(Text key, Text value, int numReduceTasks){
		
		String day=key.toString().split("@")[0];
		
		return super.getPartition(new Text(day), value, numReduceTasks);
	}
	
}
