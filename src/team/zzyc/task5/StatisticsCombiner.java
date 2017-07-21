package team.zzyc.task5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入: (day@time, url)
// 输出: (day@time, url:n)
public class StatisticsCombiner extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Hashtable<String, Integer> map=new Hashtable<>();
		for (Text val : values) {
			String url=val.toString();
			if(!map.containsKey(url))
				map.put(url, 1);
			else
				map.put(url, map.get(url)+1);
		}
		List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map.entrySet());
		String str="";
		for(int i=0;i<l.size();i++){
			str+=l.get(i).getKey()+":"+l.get(i).getValue()+" ";
		}
		context.write(_key, new Text(str));
	}

}
