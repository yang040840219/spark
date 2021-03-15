// scalastyle:off

package org.apache.spark.sql.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession


// scalastyle:off
class RDDSuite extends SparkFunSuite {

    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = spark.sparkContext

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
    
    test("rdd repartition") {
        val rdd = sc.parallelize(1 to 100, 2)
        rdd.repartition(10)
    }

}
