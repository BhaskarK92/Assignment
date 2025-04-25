package Session28_Assignment1

import org.apache.spark.sql.SparkSession

object Top_5_Destinations {

  def main(args: Array[String]): Unit = {

    println("Session 28 assignment problem 1 !!!")

    // Use new SparkSession interface in Spark
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Assignment")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    // load the dataset using the textFile method.
    val delayed_Flights_data_with_header = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session28_MLIB1\\DelayedFlights.csv")

    //creating a variable header, which holds the first line of the dataset, in our data set Sports_data.txt the first line is a header line.
    val header = delayed_Flights_data_with_header.first()

    //filter the header line from the dataset using the filter RDD
    val delayed_Flights_data = delayed_Flights_data_with_header.filter(row => row != header)

    // filter the null records from delayed flight data
    val filter_null_values = delayed_Flights_data.map(x => x.split(",")).filter(x => x!= null)

   // map column destination as key,use reduce by key for total no of each destination and sort the destination descending order
    val map_destination = filter_null_values.map(x =>(x(18),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false)

    // print the top 5 sorted destinations
    val top_5_destinations = map_destination.map(x => (x._2,x._1)).take(5).foreach(println)

    print("Top 5 most visited destinations")


  }

}
