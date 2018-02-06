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

public class SoldForOnida_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, 
	
	InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "Onida Sales In Each State");
		//Driver class name or entry point
		
		job.setJarByClass(SoldForOnida_Driver.class);
		job.setMapperClass(SoldForOnida_Mapper.class);
		job.setReducerClass(SoldForOnida_Reducer.class);
		
		
		job.setOutputKeyClass(Text.class);;
		job.setOutputValueClass(IntWritable.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result=job.waitForCompletion(true);
		
		int status = result?0:1;
		
		System.exit(status);
		
		
	}
	

}
