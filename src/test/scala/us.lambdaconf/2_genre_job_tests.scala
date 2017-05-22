package us.lambdaconf

import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.Instant

class GenreJobTest extends CodioPipelineSpec {

  behavior of "GenreJob"

  it should "parseMetadata" in {
    val metadata = Metadata(
      trackId = TrackId(1),
      title = "Metallica – Kill'em All",
      genre = "metal"
    )

    val typed = TypedMetadata(
      trackId = TrackId(1),
      title = NonEmptyString.parse("Metallica – Kill'em All").get,
      genre = Genre.Metal
    )

    GenreJob.parseMetadata(metadata) shouldEqual Some(typed)
  }

  val trigger = AfterWatermark.pastEndOfWindow()

  it should "produce plays by genre" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      PlayTrack(UserId(1), TrackId(1), msPlayed = 121L, Instant.now()),
      PlayTrack(UserId(1), TrackId(2), msPlayed = 153L, Instant.now()),
      PlayTrack(UserId(2), TrackId(1), msPlayed = 123L, Instant.now())
    ))

    val metadata = sc.parallelize(List(
      TypedMetadata(TrackId(1), NonEmptyString.parse("Metallica – Kll'em All").get, genre = Genre.Metal),
      TypedMetadata(TrackId(2), NonEmptyString.parse("Miles Davis – So What").get, genre = Genre.Jazz)
    ))

    GenreJob.playsByGenre(playTracks, metadata) should containInAnyOrder(List(
      PlayByGenre(UserId(1), Genre.Metal, count = 1),
      PlayByGenre(UserId(1), Genre.Jazz,  count = 1),
      PlayByGenre(UserId(2), Genre.Metal, count = 1)
    ))
  }
}
