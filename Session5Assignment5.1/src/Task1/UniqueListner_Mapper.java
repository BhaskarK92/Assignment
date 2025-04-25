package Task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueListner_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>

{
	private static IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
			
			throws IOException, InterruptedException 
	{
		  String[] lineArray = value.toString().split("[|]");
		
		  String listner = lineArray[1];
		  
		  context.write(new Text(listner), one);
		
	}

}