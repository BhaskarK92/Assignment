import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer 
{

  public static void main(String[] args) throws IOException
  
  {
    if (args.length != 2) 
	
	{
      System.out.println("Please provide appropriate command line arguments");
      System.exit(-1);
    }

	// create instance for properties to access producer configuraion 
    Properties props = new Properties();
	
	//Use property put to configure bootstrap server to connect kafka broke(server)
    props.put("bootstrap.servers", "localhost:9092");
	
	//Use property to configure string serializer class for key
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
	//Use property to configure string serializer class for value
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	//pass the above three configuraion to props object and instantiate a producer with string type for key and value
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
	
	
    ProducerRecord<String, String> producerRecord = null;
    
	//pass the fileName as argument first
    String fileName = args[0];
	
	//pass the delimiter as argument second
    String delimiter = args[1];
	
	
    /** The FileReader takes the fileName as a parameter. The FileReader is passed to the BufferedReader, 
	    which buffers reads lines. This is a try-with-resources statement which 
		ensures that the resource (the buffered reader) is closed at the end of the statement. **/
		
    try(BufferedReader br = new BufferedReader(new FileReader(fileName))) 
	{
		
		//for loop in the below code will read the file until it has reached the end of file
        for(String line; (line = br.readLine()) != null; ) 
		
		{
			//split the lines by delimiter and passed into array tempArray
            String[] tempArray = line.split(delimiter);
			
		    
            String topic = tempArray[0];
            String key = tempArray[1];
            String value = tempArray[2];
            
			//create a producerRecord  object with topic,key and value
            producerRecord = new ProducerRecord<String, String>(topic, key, value);
			
			//producerRecord is send to producer and producer will send it to kafka broker by calling send method
        	producer.send(producerRecord);
			
        	System.out.printf("Record sent to topic:%s. Key:%s, Value:%s\n", topic, key, value);
        }
    }
	        //after sending all the message close the producer object
            producer.close();
  } 
}
