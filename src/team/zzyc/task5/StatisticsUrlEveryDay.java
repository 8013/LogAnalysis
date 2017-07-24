package src.team.zzyc.task5;

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
 * 接口访问频次预测,
 * 给定2015-09-08.log 到 2015-09-21.log 共 14 天的日志文件,
 * 作为训练数据,设计预测算法来预测下一天(2015-09-22)
 * 每个小时窗内每个接口(请求的URL)的访问总频次。
 * 输出格式同任务 1。
 * @author zhe
 */

public class StatisticsUrlEveryDay {

	public static void run(String input, String output) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "UrlEveryDay");
		
		job.setJarByClass(StatisticsUrlEveryDay.class);
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



