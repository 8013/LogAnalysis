package team.zzyc.task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 输出: (url, time 1)
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		
		String time=splits[1].substring(splits[1].indexOf(':')+1);
		String url=splits[4];

		context.write(new Text(url), new Text(time+" "+"1"));
	}

}
