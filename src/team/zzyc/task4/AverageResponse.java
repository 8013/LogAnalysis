package team.zzyc.task4;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 统计每个接口的平均响应时间,并且以接口为分组,按照小时时间窗,
 * 输出各个时间段各个接口平均的响应时间。
 * 每个接口的统计信息是一个文 件 , 
 * 如接口/tour/category/query的统计文件命名为:tour-category-query.txt,
 * 每个文件的输出格式同任务 1。
 * @author zhe
 */

public class AverageResponse {

	public static void run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "AverageResponse");
		
		job.setJarByClass(AverageResponse.class);
		job.setMapperClass(StatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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
