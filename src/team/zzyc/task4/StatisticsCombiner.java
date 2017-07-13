package team.zzyc.task4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入: (url@time, 1 response)
// 输出: (url@time, n responses)
public class StatisticsCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int n=0,responses=0;
		for (Text val : values) {
			String []splits=val.toString().split(" ");
			n+=Integer.parseInt(splits[0]);
			responses+=Integer.parseInt(splits[1]);
		}
		
		context.write(_key, new Text(n+" "+responses));
	}

}
