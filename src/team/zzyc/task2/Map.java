package team.zzyc.task2;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object,Text,Text,Text>{
	
	public void map(Object key, Text value, Context context){
		String[] str=value.toString().split(" ");
		String IPAddress=str[0];
		String timestamp=str[1]+" "+str[2];
		//String statecode=str[str.length-3];
		String k="";
		String v="";
		k+=IPAddress;
		v+=TimeStampToTimeWindow(timestamp)+"#1";
		//v+=statecode;
		try {
			context.write(new Text(k), new Text(v));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String TimeStampToTimeWindow(String timestamp){
		String regex="[1-9][0-9]{0,3}:[0-9]{2}:[0-9]{2}:[0-9]{2}";
		Pattern p=Pattern.compile(regex);
		Matcher m=p.matcher(timestamp);
		if(m.find()){
			String res=m.group();
			//dSystem.out.println(timestamp);
			//System.out.println(res);
			String[] ans=res.split(":");
			int hour=Integer.parseInt(ans[1]);
			return String.format("%02d:00-%02d:00", hour,(hour+1)%24);
		}
		else{
			return "24:00-25:00";
		}
	}
}
