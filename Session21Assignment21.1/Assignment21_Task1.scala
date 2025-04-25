import org.apache.spark.sql.SparkSession

object Assignment21_Task1 {

//declare a case class SPORTS holding the dataset description of the sports_data.
  case class SPORTS(firstname:String,lastname:String,sports:String,medal_type:String,age:String,year:String,country:String)

  def main(args: Array[String]): Unit = {

    println("Assignment Number 21 Task1 !!!")

	// Use new SparkSession interface in Spark
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Assignment21")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

     // load the dataset using the textFile method.
    val sports_data_with_header = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session21_Spark_SQL2\\Sports_data.txt")
     
	 //creating a variable header, which holds the first line of the dataset, in our data set Sports_data.txt the first line is a header line.
    val header = sports_data_with_header.first()

	//filter the header line from the dataset using the filter RDD
    val sports_data = sports_data_with_header.filter(row => row != header)

	//For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

	//preparing a structure for the data, mapping it to the case class structure, and finally converting it to a data frame.
    val sports_data_df = sports_data.map(_.split(",")).map(x => SPORTS(x(0), x(1), x(2), x(3), x(4), x(5), x(6))).toDF

    sports_data_df.show()
    
	//creating a temporary table called “sports”
    sports_data_df.createOrReplaceTempView("sports")

	// Use spark-sql to use to query the dataframe for calculating gold winner
    val no_of_gold_winner = spark.sql("SELECT year, count(*) AS no_of_gold_medals FROM sports WHERE medal_type='gold' GROUP BY year ORDER BY year").show()

	println("Total number of Gold winners every year")
	
	// Use spark-sql to use to query the dataframe for calculating silver winner for USA
    val no_of_silver_winner =spark.sql("SELECT sports, count(*) AS silver_medals FROM sports WHERE country='USA' and medal_type='silver' GROUP BY sports ORDER BY  sports").show()
    
	println("Total no of silver winners for USA every year")
  }
}