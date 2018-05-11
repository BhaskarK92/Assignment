import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Assignment20 {
   
  //Create case classes globally to be used inside the main method for different dataset
  case class TRAVEL(id: Int, origin: String, destination: String, transport: String, distance: Int, year: Int)

  case class TRANSPORT(transport: String, amount: Int)

  case class USER(id: Int, name: String, age: Int)

   def main(args: Array[String]): Unit =
   {
     println("Assignment Number 20 !!!")

     // Use new SparkSession interface in Spark
	 val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Assignment 20 task 1 to 5 ")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
     
	 // For implicit conversions like converting RDDs and sequences  to DataFrames
     import spark.implicits._

	// Create an RDD of TRAVEL objects from a text file S20_Dataset_Holidays.txt.
    val travel = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_Holidays.txt")
    val travelDF = travel.map(_.split(",")).map(line => TRAVEL(line(0).toInt, line(1).toString,
                          line(2).toString, line(3).toString, line(4).toInt, line(5).toInt))

	//convert the RDD travelDF to a Dataframe
    val transportByAirplane = travelDF.filter(x => x.transport == "airplane").toDF

    
	// Create an RDD of TRAVEL objects from a text file S20_Dataset_Transport.txt and convert the RDD transportDF to a Dataframe
    val transportMode = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_Transport.txt")
    val transportDF = transportMode.map(_.split(",")).map(line => TRANSPORT(line(0).toString, line(1).toInt)).toDF

    // Create an RDD of TRAVEL objects from a text file S20_Dataset_User_details.txt and convert the RDD userDF to a Dataframe
    val user = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_User_details.txt")
    val userDF = user.map(_.split(",")).map(line => USER(line(0).toInt, line(1).toString, line(2).toInt)).toDF



	//Use transportByAirplane dataframe,group the year and count the value
    val air_travelers_per_year = transportByAirplane.groupBy("year").count().sort("year").show()
    println(" Total no. of air travelers per year")


    //Use transportByAirplane dataframe,group it by user and year apply summation for distance column.
    val Total_Distance_Cover_per_year = transportByAirplane.groupBy("id","year").sum("distance").orderBy("id").show()
    println("Total air distance cover by each user per year")

    //Use transportByAirplane dataframe,group it by id and add all the distance with respect to it.
    val largest_Distance_By_User = transportByAirplane.groupBy("id").sum("distance").orderBy("id").show(1)
    println("Largest distance travel by user till date ")


    //Use transportByAirplane dataframe,group it by destination column and count its value.
    val preferred_destination = transportByAirplane.groupBy("destination").count().orderBy(desc("count")).show(1)
    println("Most preferred destination for all users")


    //Join transportByAirplane and transportDF dataframes ,group it by year,origin and destination column and
	//add all the amount with repect to this column
    val revenue_per_year = transportByAirplane.join(transportDF,transportByAirplane("transport")=== transportDF("transport")).
      groupBy("year","origin","destination").sum("amount").sort(desc("sum(amount)"))show(10)
    println("Route generating most revenue per year")

   //Join transportByAirplane and transportDF dataframes ,group it by id,year and add all the amount with repect to this columns
    val amount_spent_per_year = transportByAirplane.join(transportDF,transportByAirplane("transport")=== transportDF("transport")).
      groupBy("id","year").sum("amount").orderBy("id","year").show()
    print("total amount spent by every user on air travel per year")

  }

}