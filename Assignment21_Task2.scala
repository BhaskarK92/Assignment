import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Assignment21_Task2 {
  
  //declare a case class SPORTS holding the dataset description of the sports_data.
  case class SPORTS(firstname: String, lastname: String, sports: String, medal_type: String, age: String, year: String, country: String)

  def main(args: Array[String]): Unit = {
    println("Assignment Number 21 Task 2 !!!")

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

	//Use udf method to define first_and_last_name_concat udf to concat Mr.first 2 letter of firstname and lastname
    val first_and_last_name_concat = udf((first_name: String, last_name: String) => "Mr.".concat(first_name.substring(0, 2)).concat(" ").concat(last_name))
 
    //use first_and_last_name_concat udf on sports_data_df dataframe.
    val new_sports_data_sports_df = sports_data_df.withColumn("fullName", first_and_last_name_concat(sports_data_df("firstname"), sports_data_df("lastname")))

    new_sports_data_sports_df.select("fullName","sports","medal_type","age","year","country").show()

	//Use udf method to define Rank udf to add new column on dataframe
    val Rank = udf((medal_type: String, age: Int) => {
      if (medal_type == "gold" && age >= 32) "pro"
      else if (medal_type == "gold" && age <= 31) "amateur"
      else if (medal_type == "silver" && age >= 32) "expert"
      else if (medal_type == "silver" && age <= 32) "rookie"
      else " no-level"
    })

	//apply RANK udf on sports_data_df dataframe.
    sports_data_df.withColumn("ranking", Rank(sports_data_df("medal_type"), sports_data_df("age"))).show()


  }
}