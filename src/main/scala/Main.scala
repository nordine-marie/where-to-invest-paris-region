import scala.io.Source
import scala.language.implicitConversions 
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    println("Scala mini project : Where To Invest")
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    //val columns_index = List(2,10,44)
    val data = sc.textFile("data/PC_DP_cr‚ant_logements_2017_2021.csv") // read file

    val idf_code = "11" // idf region code

    val idf_cp_data = region_cp_data(sc,idf_code)

    val idf_cp = idf_cp_data.map(line => ((line(0)),Integer.parseInt(line(2)))) // permis de construire par ville
    .reduceByKey(_+_)

    val idf_cp_by_year = idf_cp_data.map(line => ((line(0),line(1)),Integer.parseInt(line(2)))) // permis de construire par ville et année
    .reduceByKey(_+_)

    val idf_general_data =  region_general_data(sc,idf_code)
    
    val idf_merged_data = idf_general_data.map(line => (line(0),(line(1),line(2),line(3),line(4),line(5),line(6)))) // put insee code as key in general data
    .join(idf_cp) // join on key (insee_code)
    .map{ case (insee_code,(values,nb_cp)) => (insee_code, values._1,values._2,values._3,values._4,nb_cp.toString(), (nb_cp.toFloat/values._4.toFloat).toString(), values._5) } // Insee code, Postal code, City, Depart, Pop, Nb_cp, Nb_cp/Pop, GeoPoint

    val output_columns = Seq("Insee code", "Postal code", "City Name", "Department", "Population", "Number of Building Permit", "Building Score", "GeoPoint")
    val output_df = sqlc.createDataFrame(idf_merged_data)
    .toDF(output_columns:_*) // header
    .repartition(1) // only one csv file at writing

    output_df.show() // logging the dataframe

    output_df.write.option("header",true)
   .csv("file://" + sys.env.get("HOME").get + "/tmp/output4")
  }

  def region_cp_data(sc: SparkContext, region_code: String): RDD[List[String]] = {
    val data = sc.textFile("data/PC_DP_cr‚ant_logements_2017_2021.csv") // read file
    val columns_index = List(2,10,44)

    val region_cp_data = data
    .map( _.split(";")) // split csv lines into entries
    .filter(line => (line(0).slice(1,3) == region_code)) // filter only idf lines 
    .map(line => columns_index.map( index => line(index))) // project only some columns
    .filter(line => line(1).slice(1,2) != " ")
    .map(line => List(line(0).slice(1,6),line(1).slice(1,5),line(2)))
    return region_cp_data
  }

  def region_general_data(sc: SparkContext, region_code: String): RDD[List[String]] ={
    val data = sc.textFile("data/correspondance-code-insee-code-postal.csv") // read file
    val columns_index1 = List(0,1,2,3,8,9,10,16) // Insee code, Postal code, City, Depart, Pop,GeoPoint,GeoShape,region code
    val columns_index2 = List(0,1,2,3,4,5,6) // Insee code, Postal code, City, Depart, Pop,GeoPoint,GeoShape

    val region_general_data = data
    .map( _.split(";")) // split csv lines into entries
    .filter(line => (line(16) == region_code)) // filter only idf lines 
    .map(line => columns_index1.map( index => line(index))) // project only some columns
    .filter(line => line(4).toFloat >= 10.0 ) // dropping city with too low population
    .map(line => columns_index2.map( index => line(index))) // project only some columns

    return region_general_data
  }

}
