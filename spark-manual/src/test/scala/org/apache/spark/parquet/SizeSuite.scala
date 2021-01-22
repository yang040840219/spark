// scalastyle:off
package org.apache.spark.parquet

import java.net.URI
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.input_file_name

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SizeSuite extends AnyFunSuite {

	val spark = SparkSession.builder().master("local[2]").getOrCreate()

	val configuration = spark.sessionState.newHadoopConf()

	test("list files") {

		val p = "/data/delta/realtime_log/p_day=2021-01-20/p_hour=09"
		val path = new Path(p)
		val fs = path.getFileSystem(configuration)
		val df = spark.read.format("delta").load("/data/delta/realtime_log")
			.where("p_day='2021-01-20' and p_hour='09'")

		val files = df.select(input_file_name()).distinct().collect()
			.map(row => row.getString(0))
			.map(p => {
				val path = new Path(p)
				val name = path
				val stats = fs.getFileStatus(path)
				(name, stats.getLen)
			})

		files.map(item => item._2).sum // 5

		files.foreach(println) // 530097816

		val newFilesName = files.map(item => item._1).toList
		val fileStatuses = fs.listStatus(path)
		val oldFiles = fileStatuses.toList.map(status => {
			(status.getPath, status.getLen)
		}).filter(item => {
			!newFilesName.contains(item._1)
		})
		println(oldFiles.size) // 3349

		oldFiles.map(item => item._2).sum // 436672149

		def calcLocationSummary(fs: FileSystem, targetLocation: String): (Long, Long) = {
			val targetLocationPath = new Path(targetLocation)
			val summary = fs.getContentSummary(targetLocationPath)
			val totalSize = summary.getLength
			val fileCount = summary.getFileCount
			(totalSize, fileCount)
		}

		val groupFiles = oldFiles.grouped(10).toList
		val executorService = Executors.newFixedThreadPool(10)

		val fileFutures = groupFiles.map(group => {
			executorService.submit[Long](new Callable[Long] {
				override def call(): Long = {
					val head = group.head
					val hadoopConf = spark.sessionState.newHadoopConf()
					val fs = FileSystem.get(new URI(head._1.toString), hadoopConf)
					group.map(file => {
						val (fileSize, _) = calcLocationSummary(fs, file._1.toString)
						fileSize
					}).sum
				}
			})
		})
		val fileTotalSize = fileFutures.map(future => future.get(1, TimeUnit.MINUTES)).sum


		df.repartition(2)
			.write
  		.partitionBy("p_day", "p_hour")
			.option("replaceWhere", "p_day='2021-01-20' and p_hour='09'")
			.format("delta")
			.mode(SaveMode.Overwrite)
			.save("/data/tmp/realtime_log_0119")


	}

}
