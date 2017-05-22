package us.lambdaconf

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.runners.direct.DirectOptions

/** Incomplete classification of musical genres */
sealed abstract class Genre(val name: String) extends Product with Serializable

object Genre {
  final case object Metal extends Genre("metal")
  final case object Jazz extends Genre("jazz")
  final case object Rock extends Genre("rock")
  final case object Pop extends Genre("pop")

  def parse(str: String): Option[Genre] = str match {
    case Metal.name => Some(Metal)
    case Jazz.name => Some(Jazz)
    case Rock.name => Some(Rock)
    case Pop.name => Some(Pop)
    case _ => None
  }
}

final case class TypedMetadata(
  trackId: TrackId,
  title: NonEmptyString,
  genre: Genre
)

final case class PlayByGenre(
  userId: UserId,
  genre: Genre,
  count: Int
)

object GenreJob {

  // Exercises:
  //
  // 1. implement parseMetadata
  def parseMetadata(metadata: Metadata): Option[TypedMetadata] = {
    ???
  }

  // 2. count plays per user, per genre
  def playsByGenre(
    playTracks: SCollection[PlayTrack],
    metadata: SCollection[TypedMetadata]
  ): SCollection[PlayByGenre] = {
    ???
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.optionsAs[DirectOptions].setTargetParallelism(1) // special for codio

    val playTracks = sc.scalaAvroFile[PlayTrack]("in/play_track/*.avro")

    val metadata = sc
      .scalaAvroFile[Metadata]("in/metadata/*.avro")
      .flatMap(parseMetadata)

    playsByGenre(playTracks, metadata)
      .saveAsScalaAvroFile("out/plays_by_genre")
  }
}

final class NonEmptyString private(val value: String) extends AnyVal with Serializable

object NonEmptyString {
  def parse(value: String): Option[NonEmptyString] = {
    val trim = value.trim

    if (trim.isEmpty) None
    else Some(new NonEmptyString(value))
  }
}

