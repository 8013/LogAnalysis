package team.zzyc.task3;

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
 * 统计每个接口(请求的URL)访问总的频次,
 * 并且以接口为文件,按照秒为单位的时间窗,输出各个时间段各接口的访问情况。
 * 每个接口的统计信息是一个文件,
 * 如接口/tour/category/query 的统计文件命名为:tour-category-query.txt,
 * 每个文件的输出格式同任务 1。
 * @author zhe
 */

public class URLInterface {
	
	public static void run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "URLInterface");
		job.setMapperClass(StatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(StatisticsCombiner.class);
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
