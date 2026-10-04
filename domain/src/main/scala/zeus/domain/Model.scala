package zeus.domain

import zio.json._

final case class Player(id: String, name: String, level: Long, region: String, createdAt: String)
object Player:
  given JsonCodec[Player] = DeriveJsonCodec.gen[Player]

final case class PlayerInput(
    name: String,
    level: Long,
    region: String,
    createdAt: Option[String] = None
)
object PlayerInput:
  given JsonCodec[PlayerInput] = DeriveJsonCodec.gen[PlayerInput]

final case class LevelInput(delta: Long)
object LevelInput:
  given JsonCodec[LevelInput] = DeriveJsonCodec.gen[LevelInput]

final case class ScoreInput(playerId: String, score: Double)
object ScoreInput:
  given JsonCodec[ScoreInput] = DeriveJsonCodec.gen[ScoreInput]

final case class AchievementInput(name: String)
object AchievementInput:
  given JsonCodec[AchievementInput] = DeriveJsonCodec.gen[AchievementInput]

final case class LeaderboardEntry(playerId: String, score: Double)
object LeaderboardEntry:
  given JsonCodec[LeaderboardEntry] = DeriveJsonCodec.gen[LeaderboardEntry]

final case class RankResult(playerId: String, rank: Long)
object RankResult:
  given JsonCodec[RankResult] = DeriveJsonCodec.gen[RankResult]

final case class LoginResult(count: Long)
object LoginResult:
  given JsonCodec[LoginResult] = DeriveJsonCodec.gen[LoginResult]

final case class AchievementResult(added: Boolean)
object AchievementResult:
  given JsonCodec[AchievementResult] = DeriveJsonCodec.gen[AchievementResult]

final case class MembershipResult(hasAchievement: Boolean)
object MembershipResult:
  given JsonCodec[MembershipResult] = DeriveJsonCodec.gen[MembershipResult]

final case class Notification(
    id: String,
    playerId: String,
    kind: String,
    message: String,
    timestamp: String
)
object Notification:
  given JsonCodec[Notification] = DeriveJsonCodec.gen[Notification]

trait GameStore:
  import zio.Task

  def upsertPlayer(player: Player): Task[Unit]
  def getPlayer(id: String): Task[Option[Player]]
  def incrementLevel(id: String, delta: Long): Task[Option[Long]]
  def recordLogin(id: String): Task[Long]
  def addScore(playerId: String, score: Double): Task[Double]
  def top(limit: Int): Task[List[LeaderboardEntry]]

  /** Zero-based rank, matching Redis ZRANK. */
  def rank(playerId: String): Task[Option[Long]]
  def addAchievement(id: String, name: String): Task[Boolean]
  def hasAchievement(id: String, name: String): Task[Boolean]
  def commonAchievements(id1: String, id2: String): Task[List[String]]
  def batchUpsert(players: List[Player]): Task[Unit]
  def ensureNotificationGroup: Task[Unit]
  def publishNotification(notification: Notification): Task[Unit]
  def readNotifications(consumer: String): Task[List[Notification]]
  def ackNotification(id: String): Task[Unit]
