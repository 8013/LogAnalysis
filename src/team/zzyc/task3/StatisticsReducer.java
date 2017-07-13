package team.zzyc.task3;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// 输入: (url, time1 n1;time2 n2;...)
public class StatisticsReducer extends Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs<Text, Text>mos;
	
	protected void setup(Context context){
		mos=new MultipleOutputs<>(context);
	}
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 整合过程
		Hashtable<String, Integer> map=new Hashtable<>();
		for (Text val : values) {
			String[] split=val.toString().split(";");
			for(String s:split){
				int index=s.indexOf(' ');
				String time=s.substring(0,index);
				int n=Integer.parseInt(s.substring(index+1));
				if(map.containsKey(time))
					map.put(time, map.get(time)+n);
				else
					map.put(time,n);
			}
		}
		
		// 对哈希表排序
		List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map.entrySet());
		Collections.sort(l, new Comparator<HashMap.Entry<String, Integer>>() {   
			@Override
			public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
				return arg0.getKey().compareTo(arg1.getKey());
			}
		}); 
		
		// 拼接
		String str="";
		int total=0;
		for(int i=0;i<l.size();i++){
			String key=l.get(i).getKey();
			int value=l.get(i).getValue();
			str+=key+"  "+value+"\n";
			total+=value;
		}
		
		// 以接口为文件名进行输出
		String filename=_key.toString();
		if(filename.charAt(0)=='/'){
			filename=filename.substring(1).replaceAll("/", "-");
		}
		mos.write(new Text(_key.toString()), new Text(total+"\n"+str), filename+".txt");
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
	
}
