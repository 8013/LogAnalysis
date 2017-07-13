package team.zzyc.task2.alternative;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入: (ip@time, 1)
// 输出: (ip@time, n)
public class StatisticsCombiner extends Reducer<Text,  IntWritable, Text,  IntWritable> {

	public void reduce(Text _key, Iterable< IntWritable> values, Context context) throws IOException, InterruptedException {
		int total=0;
		for ( IntWritable val : values) {
			total+=val.get();
		}
		
		context.write(_key, new IntWritable(total));
	}

}
