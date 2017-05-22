package us.lambdaconf

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.runners.direct.DirectOptions

final case class PlayCount(trackId: TrackId, plays: Long)
final case class ContentHours(userId: UserId, msPlayedSum: Long)

object TrackCountJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.optionsAs[DirectOptions].setTargetParallelism(1) // special for codio

    val playTracks = sc.scalaAvroFile[PlayTrack]("in/play_track/*.avro")

    playCount(playTracks)
      .saveAsScalaAvroFile("out/play_count")

    sc.close().waitUntilDone()
  }

  // Exercises:

  // 1. inspect output with sbt shell:
  //
  //     > unit-1
  //     > avro-read in/track_play
  //     > avro-read out/play_count
  //
  // for debugging can print output directly to console, use:
  //     playCount.debug()
  def playCount(playTracks: SCollection[PlayTrack]): SCollection[PlayCount] = {
    playTracks
      .keyBy(_.trackId)
      .countByKey
      .map { case (trackId, plays) => PlayCount(trackId, plays) }
  }

  // 2. output total content hours per user
  def contentHours(playTracks: SCollection[PlayTrack]): SCollection[ContentHours] = {
    ???
  }

  // 3. output top n users by content hours
  def topUsers(n: Int, playTracks: SCollection[PlayTrack]): SCollection[ContentHours] = {
    ???
  }
}
