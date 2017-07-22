package team.zzyc.task2.alternative;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class IP {

	public static void run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf=new Configuration();
		
		Scanner in=new Scanner(new File("res/ip.txt"));
		String str="";
		while(in.hasNextLine()){
			str+=in.nextLine()+" ";
		}
		in.close();
		conf.set("ip", str);
		
		Job job=Job.getInstance(conf,"IP");

		job.setJarByClass(IP.class);
		job.setMapperClass(StatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(StatisticsCombiner.class);
		job.setPartitionerClass(StatisticsPartitioner.class);
		job.setReducerClass(StatisticsReducer.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// 检查输出目录是否存在
		Path inputPath=new Path(input), outputPath=new Path(output);
		FileSystem hdfs=FileSystem.get(URI.create(output), conf);
		if(hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}
		
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
		
        job.waitForCompletion(true);
        
        // 去掉输出文件末尾的-r-00000标志
        FileStatus[] files=hdfs.listStatus(outputPath);
        for(FileStatus f:files){
        	hdfs.rename(f.getPath(), new Path(f.getPath().toString().replaceAll("-r-00000", "")));
        }
	}
		
}
