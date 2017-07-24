package src.team.zzyc.task5;

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

public 
class StatisticsReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text>mos;
	static Hashtable<String, Integer> []map;
	static String currentDay=" ";
	
	@SuppressWarnings("unchecked")
	protected void setup(Context context){
		mos=new MultipleOutputs<>(context);
		map=(Hashtable<String, Integer>[])new Hashtable[24];
		for(int i=0;i<24;i++)
			map[i]=new Hashtable<>();
	}
	
	@SuppressWarnings("unchecked")
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String key=_key.toString();
		String day=key.split("@")[0];
		int time=Integer.parseInt(key.split("@")[1]);
		
		if(!currentDay.equals(day) && !currentDay.equals(" ")){
			String str="";
			for(int i=0;i<24;i++){
				str+=String.format("%02d:00-%02d:00 ", i,i+1);
				List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map[i].entrySet());
				Collections.sort(l, new Comparator<HashMap.Entry<String, Integer>>() {   
					@Override
					public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
						return arg0.getKey().compareTo(arg1.getKey());
					}
				}); 
				for(int j=0;j<l.size();j++)
					str+=l.get(j).getKey()+":"+l.get(j).getValue()+" ";			
				str+="\n";
			}
			mos.write(new Text(currentDay), new Text("\n"+str), currentDay+".txt");
			map=(Hashtable<String, Integer>[])new Hashtable[24];
			for(int i=0;i<24;i++)
				map[i]=new Hashtable<>();
		}
		currentDay=day;
		
		for(Text val:values){
			String []splits=val.toString().split(" ");
			for(int i=0;i<splits.length;i++){
				String url=splits[i].split(":")[0];
				int n=Integer.parseInt(splits[i].split(":")[1]);
				if(!map[time].containsKey(url))
					map[time].put(url, n);
				else
					map[time].put(url, map[time].get(url)+n);
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		String str="";
		for(int i=0;i<24;i++){
			str+=String.format("%02d:00-%02d:00 ", i,i+1);
			List<HashMap.Entry<String, Integer>> l =new ArrayList<HashMap.Entry<String, Integer>>(map[i].entrySet());
			Collections.sort(l, new Comparator<HashMap.Entry<String, Integer>>() {   
				@Override
				public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
					return arg0.getKey().compareTo(arg1.getKey());
				}
			}); 
			for(int j=0;j<l.size();j++){
				str+=l.get(j).getKey()+":"+l.get(j).getValue()+" ";	
			}
			str+="\n";
		}
		mos.write(new Text(currentDay), new Text("\n"+str), currentDay+".txt");
		mos.close();
	}
	
}
