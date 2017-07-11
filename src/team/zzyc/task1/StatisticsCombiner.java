package team.zzyc.task1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int total=0;
		for (Text val : values) {
			total+=Integer.parseInt(val.toString());
		}
		
		String key=_key.toString();
		if(key.contains("#")){
			int index=key.indexOf('#');
			String time=key.substring(0, index);
			String status=key.substring(index+1);
			context.write(new Text(time), new Text(status+":"+total));
		}
		else{
			context.write(_key, new Text(total+""));
		}
		
	}

}
