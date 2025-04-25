package Task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongListen_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>

{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
			
			throws IOException, InterruptedException 
	{
		  String[] lineArray = value.toString().split("[|]");
		  Text track_id = new Text(lineArray[1]);
		  String song  = lineArray[4];
		  
		  
		  if ( song.equals("1"))
		  
		  {  
			  IntWritable song_heard = new IntWritable(1);
			  context.write(track_id,song_heard);}
		  
		  {
			  IntWritable song_not_heard = new IntWritable(0);
			  context.write(track_id,song_not_heard);}
		  }
		
	}

