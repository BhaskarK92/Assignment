package Task4;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
public class GenericUDFConcatWS extends UDF
{
	public Text evaluate(String SEP,ArrayList<String> array)
	
	{
		if(SEP!=null)
			
		{ 
		  StringBuffer concat = new StringBuffer();
	      String s= "";
	      
	      for(int i=0;i<array.size()-1;i++)
	        { 
	    	  concat.append(array.get(i));
	          concat.append(SEP);
	        
             }
	      s=concat.append(array.get(array.size()-1)).toString();
	          
	      return new Text(s);
		  }
		   else
		    {
		    	return new Text("null");
	         }
}

	
}