package team.zzyc.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入: (url, time 1)
// 输出: (url, time n)
public class StatisticsCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Hashtable<String, Integer> map=new Hashtable<>();
		for (Text val : values) {
			String s=val.toString();
			int index=s.indexOf(' ');
			String time=s.substring(0,index);
			int n=Integer.parseInt(s.substring(index+1));
			if(map.containsKey(time))
				map.put(time, map.get(time)+n);
			else
				map.put(time,n);
		}
		
		List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map.entrySet());
		String str="";
		for(int i=0;i<l.size();i++){
			str+=l.get(i).getKey()+" "+l.get(i).getValue()+";";
		}
		context.write(_key, new Text(str));
	}

}
