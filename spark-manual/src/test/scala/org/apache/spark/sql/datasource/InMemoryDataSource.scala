// scalastyle:off
package org.apache.spark.sql.datasource

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._


class InMemoryDataSource extends TableProvider {

    private var t: Table = _

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        println("inferSchema ....")
        this.getTable(options).schema()
    }

    def getTable(options: CaseInsensitiveStringMap): Table = {
        // 根据options 确定Table
        if(t == null) {
            t = new SimpleBatchTable()
        }
        t
    }

    def getTable(options: CaseInsensitiveStringMap, schema: StructType) : Table = {
        new SimpleBatchTable()
    }

    override def getTable(schema: StructType, partitioning: Array[Transform],
                          properties: util.Map[String, String]): Table = {
        println("getTable ...")
        if(t != null) {
            t
        } else {
            this.getTable(new CaseInsensitiveStringMap(properties), schema)
        }
    }
}


class SimpleBatchTable extends Table with SupportsRead {
    override def name(): String = {
        println("name ...")
        this.getClass.toString
    }

    override def schema(): StructType = {
        println("schema ...")
        StructType(Array(StructField("value", StringType)))
    }

    override def capabilities(): util.Set[TableCapability] = {
        println("capabilities ...")
        Set(TableCapability.BATCH_READ).asJava
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        println("newScanBuilder ...")
        new SimpleScanBuilder()
    }
}

class SimpleScanBuilder extends ScanBuilder with SupportsPushDownFilters {
    override def build(): Scan = {
        println("build ...")
        new SimpleScan()
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        println(s"pushFilters ... ${filters.mkString(",")}")
        Array.empty
    }

    override def pushedFilters(): Array[Filter] = {
        Array.empty
    }
}

class SimplePartition(val start:Int, val end:Int) extends InputPartition {

}

class SimpleScan extends Scan with Batch {

    override def readSchema(): StructType = {
        println("readSchema ...")
        StructType(Array(StructField("value", StringType)))
    }

    override def planInputPartitions(): Array[InputPartition] = {
        println("planInputPartitions ...")
        Array(new SimplePartition(0, 4), new SimplePartition(5,9))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
        println("createReaderFactory ...")
        new SimplePartitionReaderFactory()
    }

    override def toBatch: Batch = {
        println("toBatch ...")
        this
    }
}

class SimplePartitionReaderFactory extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        println("createReader ...")
        new SimplePartitionReader(partition.asInstanceOf[SimplePartition])
    }
}

class SimplePartitionReader(partition: SimplePartition) extends PartitionReader[InternalRow] {

    val values = Array("1","2","3","4","5","6")
    var index = partition.start

    override def next(): Boolean = index <= partition.end && index != values.length

    override def get(): InternalRow = {
        val value = values(index)
        index = index + 1
        val row = InternalRow(UTF8String.fromString(value))
        row
    }

    override def close(): Unit = {

    }
}