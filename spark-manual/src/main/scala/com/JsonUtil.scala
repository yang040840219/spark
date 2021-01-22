// scalastyle:off
package com

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

object JsonUtil {

	private implicit val formats = Serialization.formats(NoTypeHints)

	 private val mapper = new ObjectMapper() with ScalaObjectMapper

	 mapper.registerModule(DefaultScalaModule)

	def object2json(value: Any) : String = {
		mapper.writeValueAsString(value)
	}

}
