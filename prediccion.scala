/**
 * José Luis Barrera Trancoso
 */
import org.apache.spark.SparkConf;
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions.lag
import java.sql.Timestamp

object Prediccion {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Prediccion").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Read the input data file and create a DataFrame    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("consumo.csv")      

    // Filter by Sensor DG1000420  
    val df_filtered = df.filter(col("Sensor").startsWith("DG1000420"))     
    
    // We take only measurements to calculate mean
    val cdf=df_filtered.drop("Sensor").drop("Date")     
     
    // Create a new DataFrame with the mean of each day in a temporary column Media_dia
    val meandf = df_filtered.drop("Sensor").withColumn("Media_dia", cdf.columns.map(col).reduce(_ + _).divide(cdf.columns.size))    

    // Create a new DataFrame with the column Media and copy values from Media_dia column in the previous row
    val w = org.apache.spark.sql.expressions.Window.orderBy(desc("Date"))
    val leadDf = meandf.withColumn("Media", lag("Media_dia", 1, 0).over(w))
    
    // Drop temporary column Media_dia
    val result = leadDf.drop("Media_dia")
    
    // Write DataFrame in a csv file
    result.coalesce(1).write.csv("prediccion.csv")
             
  }
  
}