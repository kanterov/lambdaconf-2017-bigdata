package us.lambdaconf

import org.joda.time.Instant

/**
  * @param userId id of user who played track
  * @param trackId id of track being played
  * @param msPlayed duration of playback in milliseconds
  * @param time when playback started
  */
final case class PlayTrack(
  userId: UserId,
  trackId: TrackId,
  msPlayed: Long,
  time: Instant
)

final case class Metadata(
  trackId: TrackId,
  title: String,
  genre: String
)

final case class TrackId(value: Long) extends AnyVal
final case class UserId(value: Long) extends AnyVal

