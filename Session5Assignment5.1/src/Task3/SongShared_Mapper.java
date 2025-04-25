package Task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongShared_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>

{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
			
			throws IOException, InterruptedException 
	{
		  String[] lineArray = value.toString().split("[|]");
		  Text track_id = new Text(lineArray[1]);
		  String song  = lineArray[2];
		  
		  
		  if ( song.equals("1"))
		  
		  {  
			  IntWritable song_shared = new IntWritable(1);
			  context.write(track_id,song_shared);}
		  
		  {
			  IntWritable song_not_shared = new IntWritable(0);
			  context.write(track_id,song_not_shared);}
		  }
		
	}

