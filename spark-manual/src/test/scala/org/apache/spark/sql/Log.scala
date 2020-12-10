// scalastyle:off
package org.apache.spark.sql

import org.apache.log4j.LogManager

/**
  * 2020/8/25
  */
trait Log extends Serializable{
    @transient lazy val log = LogManager.getLogger(this.getClass)

    def logConsole(message:Any): Unit = {
        // scalastyle:off
        println(message)
        // scalastyle:on
    }
}
