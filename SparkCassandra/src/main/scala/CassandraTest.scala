import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CassandraTest {
  
  import com.datastax.spark.connector._
  
  
  def main (args:Array[String]) {
    
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    
    val sc = new SparkContext("local[8]", "test", conf)
    
    val rdd = sc.cassandraTable("eventos", "SearchEvents")
    
    println(rdd.count)
    
    sc.textFile(path, minPartitions)
    
  }

}