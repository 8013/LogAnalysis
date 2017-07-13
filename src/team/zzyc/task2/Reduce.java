package src.team.zzyc.task2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reduce extends Reducer<Text,Text,Text,Text>{
	private MultipleOutputs<Text,Text> mos;
	
	@Override
	public void setup(Context context){
		mos=new MultipleOutputs<>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int sum=0;
		HashMap<String,Integer> map=new HashMap<>();
		for(Text t:values){
			if(t.toString().startsWith("freq:")){
				sum+=Integer.parseInt(t.toString().substring(5));
			}
			else{
				String[] str=t.toString().split("#");
				if(map.get(str[0])==null){
					map.put(str[0], Integer.parseInt(str[1]));
				}
				else{
					map.put(str[0], map.get(str[0])+Integer.parseInt(str[1]));
				}
			}
		}
		mos.write(new Text(key.toString()), new Text(""+sum), key.toString());
		Set<String> keyset=map.keySet();
		for(String k:keyset){
			System.out.println(k+":"+map.get(k));
			mos.write(new Text(k), new Text((map.get(k)).toString()), key.toString());
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
	
}
