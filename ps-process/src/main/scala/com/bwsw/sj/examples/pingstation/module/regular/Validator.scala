package com.bwsw.sj.examples.pingstation.module.regular

import com.bwsw.common.JsonSerializer
import com.bwsw.sj.common.engine.{StreamingValidator, ValidationInfo}
import org.apache.avro.Schema

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class Validator extends StreamingValidator {

  import OptionsLiterals._

  override def validate(options: String): ValidationInfo = {
    val errors = ArrayBuffer[String]()

    val jsonSerializer = new JsonSerializer
    val mapOptions = jsonSerializer.deserialize[Map[String, Any]](options)
    mapOptions.get(schemaField) match {
      case Some(schemaMap) =>
        val schemaJson = jsonSerializer.serialize(schemaMap)
        val parser = new Schema.Parser()
        if (Try(parser.parse(schemaJson)).isFailure)
          errors += s"'$schemaField' attribute contains incorrect avro schema"

      case None =>
        errors += s"'$schemaField' attribute is required"
    }

    ValidationInfo(errors.isEmpty, errors)
  }

}

object OptionsLiterals {
  val schemaField = "schema"
}