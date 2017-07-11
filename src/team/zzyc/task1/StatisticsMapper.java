package team.zzyc.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatisticsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		
		int index=splits[1].indexOf(':');
		String time=splits[1].substring(index+1, index+3);
		String status=splits[6];
		
		context.write(new Text(status), new Text("1"));
		context.write(new Text(time+"#"+status), new Text("1"));
	}
}
