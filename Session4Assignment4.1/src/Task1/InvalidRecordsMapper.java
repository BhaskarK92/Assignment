package Task1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidRecordsMapper extends Mapper<LongWritable,Text,Text,Text>

{
	public void map(LongWritable Key,Text value,Context context) 
			
			throws IOException, InterruptedException
	{
		String[] lineArray = value.toString().split("[|]");
		
		if(lineArray[0].equals("NA") || (lineArray[1].equals("NA")))
		
		{
			context.write(new Text(""), value);
		}
	}

}