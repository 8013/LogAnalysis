package src.team.zzyc.task2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPFrequence {
	public static void run(String input, String output) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"IPFrequence");
		
		job.setJarByClass(IPFrequence.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		Path inputpath=new Path(input);
		Path outputpath=new Path(output);
		FileSystem fs=FileSystem.get(URI.create(output),conf);
		if(fs.exists(outputpath)){
			fs.delete(outputpath,true);
		}
		
		
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		
		job.waitForCompletion(true);
		
		FileStatus[] files=fs.listStatus(outputpath);
        for(FileStatus f:files){
        	fs.rename(f.getPath(), new Path(f.getPath().toString().replaceAll("-r-00000", ".txt")));
        }
	}
}
