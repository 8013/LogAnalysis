package team.zzyc.task1;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计日志中各个状态码(200,404,500)出现总的频次,并且按照小时时间窗,输出各个时间段各状态码的统计情况。
 * 统计文件命名为 1.txt 输出格式为:单词与数字之间英文(:)分割。时间之间英文(-)分割,其他是空格或者空行。
 * 例如：
 * 			200:100
 * 			404:2
 * 			500:1
 * 			12:00-1:00 200:5 404:1 500:0
 * 			1:00-2:00 200:5 404:0 500:0
 *  			...		
 * @author zhe
 */

public class StatusCode {

	static FileSystem hdfs;
	static String path;
	
	public static void run(String input, String output)  throws Exception{
		path=output+"/1.txt";
		
		Configuration conf=new Configuration();
		conf.set("path", path);
		Job job=Job.getInstance(conf, "StatusCode");
		
		job.setJarByClass(StatusCode.class);
		job.setMapperClass(StatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(StatisticsCombiner.class);
		job.setReducerClass(StatisticsReducer.class);
		job.setOutputFormatClass(MyOutputFormat.class);
		
		// 检查输出目录是否存在
		Path inputPath=new Path(input), outputPath=new Path(output);
		hdfs=FileSystem.get(URI.create(output), conf);
		if(hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
		
        job.waitForCompletion(true);
        
        sort();
	}
	
	// Reduce输出时自带的排序功能不满足任务要求，对HDFS上的文件进行重新排序
	public static void sort() throws IllegalArgumentException, IOException{
        ArrayList<String> list1=new ArrayList<>();
        ArrayList<String> list2=new ArrayList<>();
        
        FSDataInputStream infile=hdfs.open(new Path(path));
        Scanner in=new Scanner(infile);
        while(in.hasNextLine()){
        	String str=in.nextLine();
        	if(!str.contains(" "))
        		list1.add(str);
        	else
        		list2.add(str);
        }
        in.close();
        infile.close();
        
        FSDataOutputStream out=hdfs.create(new Path(path), true);
        for(String s:list1)
          	out.writeBytes(s+"\n");
        for(String s:list2)
        	out.writeBytes(s+"\n");
        out.close();
	}
	
}
