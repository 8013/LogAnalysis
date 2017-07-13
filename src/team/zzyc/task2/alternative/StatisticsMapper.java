package team.zzyc.task2.alternative;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 输出: (ip@time, 1)
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		
		String ip=splits[0];
		
		int index=splits[1].indexOf(':');
		String time=splits[1].substring(index+1, index+3);
		
		context.write(new Text(ip+"@"+time), new IntWritable(1));
	}

}
