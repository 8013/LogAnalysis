package team.zzyc.task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入: (url@time, 1)
// 输出: (url, time n)
public class StatisticsCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int total=0;
		for (Text val : values) {
			total+=Integer.parseInt(val.toString());
		}
		
		String key=_key.toString();
		int index=key.indexOf('@');
		String url=key.substring(0, index);
		String time=key.substring(index+1);
		context.write(new Text(url), new Text(time+" "+total));
	}

}
