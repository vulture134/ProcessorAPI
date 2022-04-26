package domain

import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.apache.kafka.streams.scala.serialization.Serdes


object Serde {
  implicit def serde [A >: Null : Decoder : Encoder] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }
    Serdes.fromFn[A](serializer,deserializer)
  }

}