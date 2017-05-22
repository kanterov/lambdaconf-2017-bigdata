import sbt.complete._
import DefaultParsers._
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader
import sbt._

object AvroUtils {
  val fileSep = token(charClass(c => c == '/' || c == '\\')).examples("/")

  def localPathParser(entry: File): Parser[File] = {
    if (!entry.isDirectory) Parser.success(entry) else {
      val children = Option(entry.listFiles).map { c =>
        val entries = c.map { e =>
          val suffix = if (e.isDirectory) "/" else ""
          e.getName + suffix -> e
        }
        if (entry.getParentFile != null) {
          ("../" -> entry.getParentFile) +: entries
        } else
          entries
      }.getOrElse(Array.empty).toMap

      val eof = EOF map { _ => entry }
      val dot = literal(".") map { _ => entry }
      eof | token(StringBasic.examples(children.keys.toSeq.distinct:_*)).flatMap { e =>
        val entry2 = children.getOrElse(e, new File(entry, e.trim))
        val eof2 = EOF map { _ => entry2 }
        if (entry2.isFile) Parser.success(entry2) else {
          eof2 | (fileSep ~> Parser.opt(localPathParser(entry2)) map (
            _ getOrElse entry2))
        }
      } | dot
    }
  }

  val avroRead = Command(
    "avro-read", ("avro-read", "print avro file from out directory"),
    "Push a file to device from the local system"
  )(_ => Space ~ localPathParser(file("."))) { case (st, (_, file)) =>
    val files = if (file.isDirectory) {
      file.listFiles(new FileFilter { def accept(pathname: File): Boolean = pathname.getName.endsWith(".avro") }).toList
    } else {
      List(file)
    }

    files.foreach { avroFile =>
      val reader = new GenericDatumReader[GenericRecord]()
      val fileReader = DataFileReader.openReader(avroFile, reader)

      while (fileReader.hasNext) {
        println(fileReader.next())
      }
    }

    st
  }

  val avroSchema = Command(
    "avro-schema", ("avro-schema", "print avro file from out directory"),
    "Push a file to device from the local system"
  )(_ => Space ~ localPathParser(file("."))) { case (st, (_, file)) =>
    val files = if (file.isDirectory) {
      file.listFiles(new FileFilter { def accept(pathname: File): Boolean = pathname.getName.endsWith(".avro") }).toList
    } else {
      List(file)
    }

    files.headOption.foreach { avroFile =>
      val reader = new GenericDatumReader[GenericRecord]()
      val fileReader = DataFileReader.openReader(avroFile, reader)

      println(fileReader.getSchema.toString(true))
    }

    st
  }

}
