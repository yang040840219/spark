// scalastyle:off
package streaming.org.apache.spark.state

import org.apache.spark.sql.SparkSession

object SocketStreaming {
  
  def main(args: Array[String]): Unit = {
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .appName("SocketStreaming")
	  .config("spark.local.dir", "/opt/data/spark/streaming/socket")
	  .config("spark.sql.shuffle.partitions", "2")
	  .getOrCreate()
	
	val lines = spark.readStream
	  .format("socket")
	  .option("host", "localhost")
	  .option("port", "12345")
	  .option("includeTimestamp", "true")
	  .load()
  
	import spark.implicits._
	
  }
  
}
