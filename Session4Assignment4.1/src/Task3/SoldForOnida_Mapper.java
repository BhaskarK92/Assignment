package Task3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SoldForOnida_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
  
{
	           
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
			throws IOException, InterruptedException 
	
	   {
		  String[] lineArray = value.toString().split("[|]");
		  Text state_name = new Text(lineArray[3]);
		
		  String company_name = "Onida";
		    
		  if (company_name.equalsIgnoreCase(lineArray[0])) 
		  
		{			
			IntWritable sale = new IntWritable(1);
			context.write(state_name, sale);
		}
		
		
		    {
			 
			 IntWritable not_sale = new IntWritable(0);
			 context.write(state_name, not_sale); 
		
	         }

	   }
	
}


