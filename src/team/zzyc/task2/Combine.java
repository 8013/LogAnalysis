package src.team.zzyc.task2;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combine extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int sum=0;
		HashMap<String, Integer> map=new HashMap<>();
		for(Text t:values){
			//System.out.println(t);
			sum++;
			String[] str=t.toString().split("#");
			if(map.get(str[0])==null){
				map.put(str[0], Integer.parseInt(str[1]));
			}
			else{
				map.put(str[0], map.get(str[0])+1);
			}
		}
		context.write(key, new Text("freq:"+sum));
		//System.out.println("freq:"+sum);
		Set<String> keyset=map.keySet();
		for(String k:keyset){
			//System.out.println(key+":"+k+"#"+map.get(k));
			context.write(key, new Text(k+"#"+map.get(k)));
		}
	}
}
