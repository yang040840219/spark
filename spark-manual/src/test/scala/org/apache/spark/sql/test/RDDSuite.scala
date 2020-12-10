// scalastyle:off

package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}


// scalastyle:off
class RDDSuite extends SparkFunSuite {

    val testConf = new SparkConf(false)
    lazy val sc = new SparkContext("local", "test", testConf)

    test("rdd dependency") {
        val rdd = sc.parallelize(1 to 100, 2)
        val result = rdd.map(x => (x%10, x))
        .groupByKey()
        .map(x => (x._2.sum / 10 , x._1))
        .groupByKey()
        .map(x => (x._1 + 10, x._2))
        .groupByKey()
        .map(x => (x._1, x._2.size))
        result.foreach(x => println((x._1, x._2)))
    }

}
