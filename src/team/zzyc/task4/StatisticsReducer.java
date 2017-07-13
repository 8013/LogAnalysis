package team.zzyc.task4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//输入: (url@time, n responses)
public class StatisticsReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text>mos;
	static int[] num;
	static int[] responses;
	static String currentURL=" ";
	
	protected void setup(Context context){
		mos=new MultipleOutputs<>(context);
		num=new int[24];
		responses=new int[24];
	}
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String key=_key.toString();
		String url=key.split("@")[0];
		int time=Integer.parseInt(key.split("@")[1]);
		
		int n=0, r=0;
		for(Text val:values){
			String []splits=val.toString().split(" ");
			n+=Integer.parseInt(splits[0]);
			r+=Integer.parseInt(splits[1]);
		}

		if(!currentURL.equals(url) && !currentURL.equals(" ")){
			int total_n=0, total_rsp=0;
			String str="";
			for(int i=0;i<24;i++){
				str+=String.format("%02d:00-%02d:00 %.2f\n", i,i+1,((num[i]==0)?0:((double)responses[i]/num[i])));
				total_n+=num[i];
				total_rsp+=responses[i];
			}
			// 以接口为文件名进行输出
			String filename=currentURL;
			if(filename.charAt(0)=='/'){
				filename=filename.substring(1).replaceAll("/", "-");
			}
			mos.write(new Text(currentURL), new Text(String.format("%.2f", (double)total_rsp/total_n)+"\n"+str), filename+".txt");
			num=new int[24];
			responses=new int[24];
		}
		currentURL=url;
		num[time]=n;
		responses[time]=r;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		int total_n=0, total_rsp=0;
		String str="";
		for(int i=0;i<24;i++){
			str+=String.format("%02d:00-%02d:00 %.2f\n", i,i+1,((num[i]==0)?0:((double)responses[i]/num[i])));
			total_n+=num[i];
			total_rsp+=responses[i];
		}
		String filename=currentURL;
		if(filename.charAt(0)=='/'){
			filename=filename.substring(1).replaceAll("/", "-");
		}
		mos.write(new Text(currentURL), new Text(String.format("%.2f", (double)total_rsp/total_n)+"\n"+str), filename+".txt");
		mos.close();
	}
	
}
