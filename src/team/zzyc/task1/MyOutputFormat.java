package team.zzyc.task1;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutputFormat extends FileOutputFormat<Text, Text> {

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		//自定义输出路径及文件名
		FileSystem fs=FileSystem.get(URI.create(job.getConfiguration().get("path")), job.getConfiguration());
        final FSDataOutputStream out = fs.create(new Path(job.getConfiguration().get("path")));  
        
        RecordWriter<Text, Text> recordWriter=new RecordWriter<Text, Text>() {
			@Override
			public void write(Text key, Text value) throws IOException, InterruptedException {
				if(key.toString().length()==3)
					out.writeBytes(key.toString()+":"+value.toString()+"\n");
				else
					out.writeBytes(key.toString()+" "+value.toString()+"\n");
			}
			
			@Override
			public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
				if(out!=null)
					out.close();
			}
		};
		return recordWriter;
	}
}
