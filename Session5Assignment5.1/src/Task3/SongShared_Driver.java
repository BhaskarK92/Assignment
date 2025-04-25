package Task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SongShared_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	
	{
		// TODO Auto-generated method stub

		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "No of times song was shared");
		
		
		job.setJarByClass(SongShared_Driver.class);
		job.setMapperClass(SongShared_Mapper.class);
		job.setReducerClass(SongShared_Reducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);;
		job.setOutputValueClass(IntWritable.class);
		
		
	
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result=job.waitForCompletion(true);
		
		int status = result?0:1;
		
		System.exit(status);
		
		
	}
	

}
