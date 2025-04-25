import org.apache.spark.sql.SparkSession


object  MaximumDistanceCoverByAgeGroupPerYear
{

  def main(args: Array[String]): Unit =

  {   println("Assignment Number 20 !!!")
    
	// Use new SparkSession interface in Spark
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Assignment 20 task no 7 ")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

	// For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._
    
    // Create an RDD of  from a text file S20_Dataset_Holidays.txt.
    val TRAVEL = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_Holidays.txt")
    val travel = TRAVEL.map(x => (x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2),x.split(",")(3),
                                  x.split(",")(4).toInt,x.split(",")(5).toInt))
	
	// Create an RDD of  from a text file S20_Dataset_Transport.txt.
    val TRANSPORT = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_Transport.txt")
    val transport = TRANSPORT.map(x => (x.split(",")(0),x.split(",")(1).toInt))
	
	// Create an RDD of  from a text file S20_Dataset_User_details.txt.
    val USER =spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\AcadgildSessions\\Session20_Spark_SQL1\\s20\\S20_Dataset_User_details.txt")
    val user = USER.map(x => (x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2).toInt


    // create an RDD AgeGroup from user to get different age-groups from age column.
    val AgeGroup = user.map(x => x._1 -> {if(x._3<20) "20" else if(x._3>35) "35" else "20-35" })
    
    // create an RDD travelMap from travel to map id as key and (distance and year) as value 
    val travelMap = travel.map(x => (x._1 -> (x._6 ,x._5)))
   
    // create an RDD ageTravelJoin to join AgeGroup and travelMap
    val ageTravelJoin = AgeGroup.join(travelMap)
    
	// create an RDD ageTravelMap to map (year and age-groups) as key and distance as a value
    val ageTravelMap = ageTravelJoin.map(x => (x._2._1 ,x._2._2._1) -> x._2._2._2)
    
	// create an RDD to aggregate the keys year and age-groups 
    val  ageTravelReduce = ageTravelMap.reduceByKey((x,y)=> x+y).sortByKey()
	
    
	//convert the RDD yearGroupSort to a Dataframe
    val yearGroupSort = ageTravelReduce.map( x => (x._1._2,x._1._1,x._2)).toDF

    /* Now we have a dataframe yearGroupSort with data in below format
	  |  _1|   _2|  _3|
      +----+-----+----+
      |1990|   20| 200|
      |1990|20-35|1000|
      |1991|20-35| 800|    */
	  
	  //Now we use spaek-sql to get the output....

    val newName = Seq("year","ageGroup","Distance")
    //Schema of yearGroupSort is (._1,._2,._3),convert it into (year,ageGroup,Distance)	in yearGroupSortNew
    val yearGroupSortNew = yearGroupSort.toDF(newName: _*)

	// to check the new shema of yearGroupSortNew Data Frame
    yearGroupSortNew.printSchema() 
	// Register the DataFrame as a temporary view AGEGROUP
    yearGroupSortNew.createOrReplaceTempView("AGEGROUP") 
    
	// RUN SQL statements by using the sql methods provided by Spark to get the desired result from view AGEGROUP
    val max_distance_per_year = spark.sql("SELECT a.*FROM AGEGROUP a " +
                                                  "INNER JOIN " +
                                                  "(SELECT year, MAX(distance) AS max FROM AGEGROUP  " +
                                                  "GROUP BY year) b " +
                                                  "ON a.year = b.year " +
                                                  "AND a.distance = b.max ").show()

    println("Above results shows the age group travelling the most every year")

  }
}















