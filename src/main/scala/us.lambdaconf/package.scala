package us

import com.sksamuel.avro4s._
import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.twitter.chill.MeatLocker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TypeDescriptor
import org.joda.time.{Instant, LocalDateTime}
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.Future
import scala.reflect.ClassTag

package object lambdaconf {

  def testStream[A: ClassTag](sc: ScioContext)(f: TestStream.Builder[A] => TestStream.Builder[A]): SCollection[A] = {
    val cls = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val builder = org.apache.beam.sdk.testing.TestStream.create(
      sc.pipeline.getCoderRegistry.getDefaultCoder(TypeDescriptor.of(cls))
    )

    sc.wrap(sc.pipeline.apply(f(builder).advanceWatermarkToInfinity()))
  }

  // adhoc scio-scavro integration

  implicit class Avro4sScioContextSyntax(sc: ScioContext) {
    def scalaAvroFile[A: ClassTag](path: String)(implicit schema: SchemaFor[A], from: FromRecord[A]): SCollection[A] = {
      val ml = MeatLocker(from) // fix serialization issues
      sc.avroFile[GenericRecord](path, schema()).map(ml.get(_))
    }
  }

  implicit class Avro4sSCollectionSyntax[A](self: SCollection[A]) {
    def saveAsScalaAvroFile(path: String, numShards: Int = 0)(implicit schema: SchemaFor[A], to: ToRecord[A]): Future[Tap[GenericRecord]] = {
      val ml = MeatLocker(to) // fix serialization issues

      // don't use in production, each GenericData.Record has reference to Schema
      // resulting into huge serialization overhead
      //
      // TODO tweak serialization

      self
        .map(ml.get(_))
        .saveAsAvroFile(path, schema = schema(), numShards = numShards)
    }
  }

  implicit object DateTimeToSchema extends ToSchema[LocalDateTime] {
    protected val schema = Schema.create(Schema.Type.STRING)
  }

  implicit object DateTimeToValue extends ToValue[LocalDateTime] {
    override def apply(value: LocalDateTime): Any = {
      value.toString(ISODateTimeFormat.dateTime())
    }
  }

  implicit object DateTimeFromValue extends FromValue[LocalDateTime] {
    def apply(value: Any, field: Field): LocalDateTime = {
      val str = FromValue.StringFromValue(value, field)
      ISODateTimeFormat.dateTime().parseLocalDateTime(str)
    }
  }

  implicit object InstantToSchema extends ToSchema[Instant] {
    protected val schema = Schema.create(Schema.Type.LONG)
  }

  implicit object InstantToValue extends ToValue[Instant] {
    override def apply(value: Instant): Any = {
      value.getMillis
    }
  }

  implicit object InstantFromValue extends FromValue[Instant] {
    def apply(value: Any, field: Field): Instant = {
      val instant = FromValue.LongFromValue(value, field)
      new Instant(instant)
    }
  }
}
