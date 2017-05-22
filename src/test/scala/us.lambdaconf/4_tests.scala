package us.lambdaconf

import algebra.CommutativeSemigroup
import cats.kernel.Eq
import cats.kernel.laws.GroupLaws
import com.sksamuel.avro4s.RecordFormat
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.joda.time.Instant
import org.scalacheck.{Arbitrary, Prop}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.typelevel.discipline.scalatest.Discipline

class T01EndToEndTest extends CodioPipelineSpec {
  "TrackCountJob" should "work" in {
    val playTracks = List(
      PlayTrack(
        UserId(14L),
        TrackId(11L),
        msPlayed = 187L,
        time = Instant.parse("2017-04-21T00:02:13")
      )
    ).map(RecordFormat[PlayTrack].to)

    val trackCounts = List(
      PlayCount(TrackId(11L), plays = 1L)
    ).map(RecordFormat[PlayCount].to)

    JobTest[TrackCountJob.type]
      .args("--targetParallelism=1")
      .args("--arg1=value")
      .input(AvroIO[GenericRecord]("in/play_track/*.avro"), playTracks)
      .output(AvroIO[GenericRecord]("out/play_count")) { output =>
        output should containInAnyOrder(trackCounts)
      }.run()
  }
}

class T02TransformTest extends CodioPipelineSpec {
  "trackCounts" should "work" in runWithContext { sc =>
    val playTracks = sc.parallelize(List(
      PlayTrack(
        UserId(14L),
        TrackId(11L),
        msPlayed = 187L,
        time = Instant.parse("2017-04-21T04:12:41")
      )
    ))

    TrackCountJob.playCount(playTracks) should containInAnyOrder(List(
      PlayCount(TrackId(11L), plays = 1L)
    ))
  }
}

class T03UnitTest extends CodioPipelineSpec {
  "TypedMetadata" should "have non-empty title" in {
    val metadata = Metadata(TrackId(1L), title = "", genre = "metal")

    GenreJob.parseMetadata(metadata) shouldEqual None
  }
}

/** free tests for Semigroup */
class T04SemigroupPropertyTest extends FunSuite with Discipline {
  case class Stats(metal: Int, rock: Int, jazz: Int, pop: Int)

  implicit val semigroup = new CommutativeSemigroup[Stats] {
    def combine(x: Stats, y: Stats): Stats = Stats(
      metal = x.metal + y.metal,
      rock = x.rock + y.rock,
      jazz = x.jazz + y.jazz,
      pop = x.pop + y.pop
    )
  }

  implicit val arbStats = Arbitrary {
    Arbitrary.arbTuple4[Int, Int, Int, Int].arbitrary.map(Stats.tupled)
  }

  implicit val eq = Eq.fromUniversalEquals[Stats]

  // one line to check them all
  checkAll("Stats", GroupLaws[Stats].commutativeSemigroup)
}

/** free "doesn't crash" tests */
class T04DoesntCrashPropertyTest extends CodioPipelineSpec with Checkers {
  implicit val arbPlayTrack = Arbitrary {
    Arbitrary.arbTuple4[Long, Long, Long, Long].arbitrary
      .map { case (a, b, c, d) => PlayTrack(UserId(a), TrackId(b), c, new Instant(d)) }
  }

  "TrackCountJob" should "not crash" in {
    def prop(playTracks: List[PlayTrack]): Prop = Prop.secure {
      runWithContext { sc =>
        TrackCountJob.playCount(sc.parallelize(playTracks))
      }

      Prop.passed
    }

    check(prop _)
  }
}