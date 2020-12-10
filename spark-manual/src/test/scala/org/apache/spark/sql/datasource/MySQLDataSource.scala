// scalastyle:off
package org.apache.spark.sql.datasource

import java.sql.DriverManager
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
  * 2020/8/17
  */
class MySQLDataSource extends TableProvider {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        println("inferSchema ...........")
        this.getTable(null, Array.empty[Transform], options).schema()
    }

    override def getTable(schema: StructType, partitioning: Array[Transform],
                          properties: util.Map[String, String]): Table = {
        println("getTable .....")
        new MySQLTable()
    }
}

class MySQLTable extends SupportsWrite {
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        println("newWriteBuilder ...")
        new MySQLWriteBuilder()
    }

    override def name(): String = {
        this.getClass.toString
    }

    override def schema(): StructType = {
        println("schema ...")
        new StructType().add("name", StringType).add("password", StringType).add("role", IntegerType)
    }

    override def capabilities(): util.Set[TableCapability] = {
        println("capabilities ...")
        Set(TableCapability.BATCH_WRITE,
            TableCapability.TRUNCATE).asJava
    }
}

class MySQLWriteBuilder extends WriteBuilder {
    override def buildForBatch(): BatchWrite = {
        println("buildForBatch ...")
        new MySQLBatchWrite()
    }
}

class MySQLBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        println("createBatchWriterFactory ...")
        new MySQLBatchWriterFactory(info)
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {

    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {

    }
}

class MySQLBatchWriterFactory(info: PhysicalWriteInfo) extends DataWriterFactory {
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
        new MySQLBatchWriter(partitionId, taskId)
    }
}


object WriteSucceeded extends WriterCommitMessage

class MySQLBatchWriter(partitionId: Int, taskId: Long) extends DataWriter[InternalRow] {

    val url = "jdbc:mysql://localhost/test"
    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection(url, "root", "123456")
    val statement = "insert into t_user(name, password, role_id) values(?,?,?)"
    val prepareStatement = connection.prepareStatement(statement)

    override def write(record: InternalRow): Unit = {
        val name = record.getString(0)
        val password = record.getString(1)
        val role = record.getInt(2)
        prepareStatement.setString(1, name)
        prepareStatement.setString(2, password)
        prepareStatement.setInt(3, role)
        prepareStatement.execute()
    }

    override def commit(): WriterCommitMessage = {
        WriteSucceeded
    }

    override def abort(): Unit = {

    }

    override def close(): Unit = {

    }
}