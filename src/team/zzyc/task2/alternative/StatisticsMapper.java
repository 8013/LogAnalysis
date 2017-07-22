package team.zzyc.task2.alternative;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 输出: (ip@time, 1)
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Hashtable<String, Integer> ip2num;
	
	public void setup(Context context) {
		String []ips=context.getConfiguration().get("ip").split(" ");
		ip2num=new Hashtable<>();
		for(String ip:ips){
			ip2num.put(ip, ip2num.size());
		}
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String str=ivalue.toString().replaceAll(" \\S+\" " , " ");
		String[] splits=str.split(" ");
		
		String ip=splits[0];
		
		int index=splits[1].indexOf(':');
		String time=splits[1].substring(index+1, index+3);
		
		context.write(new Text(ip2num.get(ip)+"@"+time), new IntWritable(1));
	}

}
