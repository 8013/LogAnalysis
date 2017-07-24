package src.team.zzyc.task5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// 输出: (day@time, url)
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String day=((FileSplit)context.getInputSplit()).getPath().getName();
		day=day.substring(8,10);
		
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		if(splits.length!=9)
			return;
		
		int index=splits[1].indexOf(':');
		String time=splits[1].substring(index+1, index+3);
		String url=splits[4];
		
		context.write(new Text(day+"@"+time), new Text(url));
	}

}
