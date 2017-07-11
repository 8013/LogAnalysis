package team.zzyc.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int key=Integer.parseInt(_key.toString());
		
		if(0<=key && key <=23){
			String str=Integration(values);
			String time=String.format("%02d:00-%02d:00", key,key+1);
			context.write(new Text(time), new Text(str));
		}
		else{
			int total=0;
			for (Text val : values) 
				total+=Integer.parseInt(val.toString());
			context.write(_key, new Text(total+""));
		}
	}
	
	// 整合每个时间段各状态码的数量
	public String Integration(Iterable<Text> values){
		Hashtable<String, Integer> map=new Hashtable<>();
		for (Text val : values) {
			String s=val.toString();
			int index=s.indexOf(':');
			String status=s.substring(0,index);
			int v=Integer.parseInt(s.substring(index+1));
			if(map.containsKey(status))
				map.put(status, map.get(status)+v);
			else
				map.put(status,v);
		}
		
		// 对哈希表排序
		List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map.entrySet());
		Collections.sort(l, new Comparator<HashMap.Entry<String, Integer>>() {   
			@Override
			public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
				return arg0.getKey().compareTo(arg1.getKey());
			}
		}); 
		
		String str="";
		for(int i=0;i<l.size();i++){
			str+=l.get(i).getKey()+":"+l.get(i).getValue()+" ";
		}
		return str;
	}
	
}
