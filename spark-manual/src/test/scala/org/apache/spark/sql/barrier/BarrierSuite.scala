// scalastyle:off

package org.apache.spark.sql.barrier

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession


/**
	* 不支持 resource dynamic allocation
	* stage 运行时包含的tasks 需要同时运行， 每个task之间是有依赖关系的，一个task 失败该stage 中的task 全部重新执行
	*/
class BarrierSuite extends SparkFunSuite {

	test("simple barrier") {

		val spark = SparkSession.builder()
			.master("local[2]")
			.getOrCreate()

		val df = spark.range(0, 1000).repartition(4)
		val barrierRDD = df.rdd.barrier()

		val cnt = barrierRDD.mapPartitions(v => v).count()

		println(s"cnt:$cnt")
	}

}
