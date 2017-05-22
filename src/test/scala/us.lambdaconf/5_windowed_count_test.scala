package us.lambdaconf

import com.spotify.scio.streaming._
import com.spotify.scio.values._
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.format._
import org.joda.time._

case class WindowedContentHours(timestamp: Instant, plays: Long)

class WindowedContentHoursTest extends CodioPipelineSpec {

  def minutes(x: Int): Duration = Duration.standardMinutes(x)
  def days(x: Int): Duration = Duration.standardDays(x)

  behavior of "WindowedContentHoursJob"

  /** Function to test */
  def windowedContentHours(playTracks: SCollection[PlayTrack]): SCollection[WindowedContentHours] = {
    val options = WindowOptions[IntervalWindow](
      // allowedLateness = ...,
      // trigger = ...,
      // accumulationMode = ...
    )

    playTracks
      .timestampBy(_.time)
      .withFixedWindows(minutes(10), options = options)
      .map(_.msPlayed)
      .sum
      .withTimestamp
      .map { case (msPlayedSum, instant) => WindowedContentHours(instant, msPlayedSum) }
  }

  def playTrack(time: String, msPlayed: Long): TimestampedValue[PlayTrack] = {
    val t = PlayTrack(UserId(1), TrackId(1), msPlayed = msPlayed, Instant.parse(time))
    TimestampedValue.of(t, Instant.parse(time))
  }

  it should "run in batch mode" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      playTrack("2017-04-23T01:00:00.000Z", msPlayed = 61),
      playTrack("2017-04-23T01:01:00.000Z", msPlayed = 81),
      playTrack("2017-04-23T01:10:00.000Z", msPlayed = 91)
    )).timestampBy(_.getTimestamp).map(_.getValue)

    windowedContentHours(playTracks) should containInAnyOrder(List(
      WindowedContentHours(Instant.parse("2017-04-23T01:09:59.999Z"), 61 + 81),
      WindowedContentHours(Instant.parse("2017-04-23T01:19:59.999Z"), 91)
    ))
  }

  it should "run in streaming mode" in runWithContext { sc =>
    val playTracks = testStream[PlayTrack](sc) { s =>
      s.addElements(
        playTrack("2017-04-23T01:00:00.000Z", msPlayed = 61),
        playTrack("2017-04-23T01:01:00.000Z", msPlayed = 71),
        playTrack("2017-04-23T01:02:00.000Z", msPlayed = 81),
        playTrack("2017-04-23T01:10:00.000Z", msPlayed = 91)
      )
    }

    windowedContentHours(playTracks) should containInAnyOrder(List(
      WindowedContentHours(Instant.parse("2017-04-23T01:09:59.999Z"), 61 + 71 + 81),
      WindowedContentHours(Instant.parse("2017-04-23T01:19:59.999Z"), 91)
    ))
  }

  it should "run in streaming mode with late data" in runWithContext { sc =>
    val playTracks = testStream[PlayTrack](sc) { s =>
      s.addElements(
        playTrack("2017-04-23T01:00:00.000Z", msPlayed = 61)
      ).advanceWatermarkTo(
        Instant.parse("2017-04-23T02:00:00.000Z")
      ).addElements(
        playTrack("2017-04-23T01:01:00.000Z", msPlayed = 71),
        playTrack("2017-04-23T01:02:00.000Z", msPlayed = 81),
        playTrack("2017-04-23T01:10:00.000Z", msPlayed = 91)
      )
    }

    windowedContentHours(playTracks) should containInAnyOrder(List(
      WindowedContentHours(Instant.parse("2017-04-23T01:09:59.999Z"), 61),
      WindowedContentHours(Instant.parse("2017-04-23T01:19:59.999Z"), 91),
      WindowedContentHours(Instant.parse("2017-04-23T01:09:59.999Z"), 61 + 71 + 81)
    ))
  }

  it should "run in streaming mode, drop event if late more than 1 day" in runWithContext { sc =>
    val playTracks = testStream[PlayTrack](sc) { s =>
      s.advanceWatermarkTo(
        // 2 days have passed
        Instant.parse("2017-04-25T01:00:00.000Z")
      ).addElements(
        // get event late event and don't count it
        playTrack("2017-04-23T01:01:00.000Z", msPlayed = 61)
      )
    }

    windowedContentHours(playTracks) should beEmpty
  }
}