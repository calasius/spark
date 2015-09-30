

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.mongodb.hadoop.MongoInputFormat
import org.bson.BSONObject
import com.mongodb.util.JSON
import org.bson.BSONObject
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.bson.BasicBSONObject
import scala.Array
import org.bson.BSON
import com.mongodb.casbah.Imports._
import java.sql.Date
import scala.collection.mutable.TreeSet
import scala.collection.mutable.HashSet
import com.mongodb.hadoop.MongoOutputFormat

object CassandraCacheAnalisis {

  def main(args: Array[String]) {

    val mongoConfig = new Configuration()

    mongoConfig.set("mongo.input.uri", "mongodb://localhost:27017/eventos.searchEvents")

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "CassandraCacheAnalisis", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,
      classOf[MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])

    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/eventos.results")

    val rdd = documents.map({ case (key, value) => createMapObject(value) }).reduceByKey({ case (requestInfo1, requestInfo2) => combine(requestInfo1, requestInfo2)})
    .map({case (key, value) => calculateHitCollitionStatistics(key, value) })
    val values = rdd.collect()
    val size = values.length
    
    rdd.saveAsNewAPIHadoopFile(
    "file:///this-is-completely-unused",
    classOf[Object],
    classOf[BSONObject],
    classOf[MongoOutputFormat[Object, BSONObject]],
    outputConfig)

    println(values(0).toString)

  }

  def createMapObject(record: SearchRecord): Tuple2[Int, RequestInfo] = {
    (record.getHash(), new RequestInfo(List(record)))
  }

  def combine(requestInfo1: RequestInfo, requestInfo2: RequestInfo): RequestInfo = {
    val combined = requestInfo1.getSearchRecords ++ requestInfo2.getSearchRecords()
    new RequestInfo(combined)
  }
  
  def calculateHitCollitionStatistics(id: Int, reducedValue: RequestInfo): Tuple2[Object,BSONObject] = {
   
    val sortedHitInfo = reducedValue.getSearchRecords().sortWith({case (o1,o2) => o1.getTimestamp() < o2.getTimestamp()})
    val concurrentUserIdSet = new HashSet[String]
    var contadores: List[Int] = List()
    var totalHit = 0
    val allUserIds = new HashSet[String]
    
    for (record <- sortedHitInfo) yield {
      if (record.getHit()) {
    	  totalHit = totalHit + 1
    	  concurrentUserIdSet += record.getUserId()
    	  allUserIds += record.getUserId()
      } else {
        contadores = concurrentUserIdSet.size::concurrentUserIdSet.size::contadores
        concurrentUserIdSet.clear()
      }
    }
    
    if (concurrentUserIdSet.size > 0) {
      contadores = concurrentUserIdSet.size::concurrentUserIdSet.size::contadores
    }
    
    val maxConcurrentUsers = contadores.max
    
    (id.asInstanceOf[Object],new RequestStatistics(maxConcurrentUsers, allUserIds.size, totalHit, sortedHitInfo.size))
  }
  
  
  class SearchRecord(hashRequest: Int, hit: Boolean, userId: String, timestamp: Long) extends Serializable {
    def getHash() = hashRequest
    def getHit() = hit
    def getUserId() = userId
    def getTimestamp() = timestamp
  }
  
  class RequestInfo(searchRecords: List[SearchRecord]) extends Serializable {
    def getSearchRecords() = searchRecords
  }
  
  class RequestStatistics(maxConcurrentUsers: Int, userAmount: Int, totalHits: Int, totalSearch: Int) extends Serializable {
    def getMaxConcurrentUsers() = maxConcurrentUsers
    def getUserAmount() = userAmount
    def getTotalHits() = totalHits
    def getTotalSearch() = totalSearch
  }
  
  
  implicit def BSONObjectToSearchRecord(bson: BSONObject): SearchRecord = {
    val hashRequest = bson.get("hashRequest").asInstanceOf[Int]
    val hit = bson.get("hit").asInstanceOf[Boolean]
    var userId = bson.get("userId").asInstanceOf[String]
    if (userId == null) userId = "WITHOUT_USER_ID"
    val timestamp = bson.get("timestamp").asInstanceOf[Long]
	new SearchRecord(hashRequest, hit, userId, timestamp)
  }
  
  implicit def RequestStatisticsToBSONObject(requestStatistics: RequestStatistics): BSONObject = {
    val bson = new BasicBSONObject()
    bson.put("maxConcurrentUsers", requestStatistics.getMaxConcurrentUsers)
    bson.put("userAmount", requestStatistics.getUserAmount)
    bson.put("totalHits", requestStatistics.getTotalHits)
    bson.put("totalSearch", requestStatistics.getTotalSearch)
    bson
  } 
  

}