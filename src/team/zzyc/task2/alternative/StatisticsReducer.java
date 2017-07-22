package team.zzyc.task2.alternative;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// 输入: (ip@time, n)
public class StatisticsReducer extends Reducer<Text, IntWritable, Text, Text> {
	
	private MultipleOutputs<Text, Text>mos;
	private Hashtable<String, String> num2ip;
	static int[] num;
	static String currentIP=" ";
	
	protected void setup(Context context){
		mos=new MultipleOutputs<>(context);
		num=new int[24];

		String []ips=context.getConfiguration().get("ip").split(" ");
		num2ip=new Hashtable<>();
		for(String ip:ips){
			num2ip.put(num2ip.size()+"", ip);
		}
	}
	
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String key=_key.toString();
		String ip=key.split("@")[0];
		int time=Integer.parseInt(key.split("@")[1]);
		
		int n=0;
		for(IntWritable val:values){
			n+=val.get();
		}
		
		if(!currentIP.equals(ip) && !currentIP.equals(" ")){
			int total_n=0;
			String str="";
			for(int i=0;i<24;i++){
				str+=String.format("%02d:00-%02d:00 %d\n", i,i+1,num[i]);
				total_n+=num[i];
			}
			
			mos.write(new Text(num2ip.get(currentIP)), new Text(total_n+"\n"+str), num2ip.get(currentIP)+".txt");			
			num=new int[24];
		}
		currentIP=ip;
		num[time]=n;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		int total_n=0;
		String str="";
		for(int i=0;i<24;i++){
			str+=String.format("%02d:00-%02d:00 %d\n", i,i+1,num[i]);
			total_n+=num[i];
		}
		
		mos.write(new Text(num2ip.get(currentIP)), new Text(total_n+"\n"+str), num2ip.get(currentIP)+".txt");			
		mos.close();
	}
	
}
