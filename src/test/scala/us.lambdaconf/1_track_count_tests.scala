package us.lambdaconf

import org.joda.time._

class TrackCountJobTest extends CodioPipelineSpec {

  behavior of "playCount"

  it should "counts tracks by trackId" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      PlayTrack(UserId(1), TrackId(1), msPlayed = 121L, Instant.now()),
      PlayTrack(UserId(1), TrackId(2), msPlayed = 153L, Instant.now()),
      PlayTrack(UserId(2), TrackId(1), msPlayed = 123L, Instant.now())
    ))

    TrackCountJob.playCount(playTracks) should containInAnyOrder(List(
      PlayCount(TrackId(1), plays = 2),
      PlayCount(TrackId(2), plays = 1)
    ))
  }

  behavior of "contentHours"

  it should "sums msPlayed by userId" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      PlayTrack(UserId(1), TrackId(1), msPlayed = 121L, Instant.now()),
      PlayTrack(UserId(1), TrackId(2), msPlayed = 153L, Instant.now()),
      PlayTrack(UserId(2), TrackId(1), msPlayed = 123L, Instant.now())
    ))

    TrackCountJob.contentHours(playTracks) should containInAnyOrder(List(
      ContentHours(UserId(1), msPlayedSum = 274),
      ContentHours(UserId(2), msPlayedSum = 123)
    ))
  }

  behavior of "topUsers"

  it should "give top 2 users by content hours" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      PlayTrack(UserId(1), TrackId(1), msPlayed = 100L, Instant.now()),
      PlayTrack(UserId(1), TrackId(2), msPlayed = 200L, Instant.now()),
      PlayTrack(UserId(2), TrackId(1), msPlayed = 200L, Instant.now()),
      PlayTrack(UserId(3), TrackId(1), msPlayed = 150L, Instant.now())
    ))

    TrackCountJob.topUsers(2, playTracks) should containInAnyOrder(List(
      ContentHours(UserId(1), msPlayedSum = 300L),
      ContentHours(UserId(2), msPlayedSum = 200L)
    ))
  }
}
