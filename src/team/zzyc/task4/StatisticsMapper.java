package team.zzyc.task4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 输出: (url@time, 1 response)
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		
		int index=splits[1].indexOf(':');
		String time=splits[1].substring(index+1, index+3);
		String url=splits[4];
		String response=splits[8];
		
		context.write(new Text(url+"@"+time), new Text("1"+" "+response));
	}

}
