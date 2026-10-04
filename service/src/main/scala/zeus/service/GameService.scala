package zeus.service

import zeus.domain._
import zio._
import zio.stream._

final class GameService(store: GameStore, notifications: Hub[Notification]):
  def notificationStream: ZStream[Any, Nothing, Notification] =
    ZStream.fromHub(notifications)

  private def requireId(id: String): Task[Unit] =
    ZIO
      .fail(new IllegalArgumentException("id must not be blank"))
      .when(id.isBlank)
      .unit

  def upsertPlayer(id: String, input: PlayerInput): Task[Player] =
    for
      _ <- requireId(id)
      _ <- ZIO
        .fail(
          new IllegalArgumentException(
            "name and region must not be blank; level must be nonnegative"
          )
        )
        .when(input.name.isBlank || input.region.isBlank || input.level < 0)
      existing <- store.getPlayer(id)
      now <- Clock.instant
      player = Player(
        id,
        input.name,
        input.level,
        input.region,
        existing.map(_.createdAt).orElse(input.createdAt).getOrElse(now.toString)
      )
      _ <- store.upsertPlayer(player)
      _ <- ZIO.when(existing.nonEmpty)(
        store.publishNotification(
          Notification("", id, "profile_updated", "Profile updated", now.toString)
        )
      )
    yield player

  def getPlayer(id: String): Task[Option[Player]] =
    requireId(id) *> store.getPlayer(id)

  def changeLevel(id: String, delta: Long): Task[Option[Player]] =
    for
      _ <- requireId(id)
      level <- store.incrementLevel(id, delta)
      player <- level match
        case None    => ZIO.none
        case Some(_) =>
          for
            now <- Clock.instant
            _ <- store.publishNotification(
              Notification("", id, "level_changed", s"Level changed by $delta", now.toString)
            )
            profile <- store.getPlayer(id)
          yield profile
    yield player

  def login(id: String): Task[LoginResult] =
    requireId(id) *> store.recordLogin(id).map(LoginResult(_))

  def addScore(input: ScoreInput): Task[LeaderboardEntry] =
    for
      _ <- requireId(input.playerId)
      _ <- ZIO
        .fail(new IllegalArgumentException("score must be finite"))
        .when(!input.score.isFinite)
      score <- store.addScore(input.playerId, input.score)
    yield LeaderboardEntry(input.playerId, score)

  def top(limit: Int): Task[List[LeaderboardEntry]] =
    ZIO
      .fail(new IllegalArgumentException("limit must be between 1 and 100"))
      .when(limit < 1 || limit > 100) *> store.top(limit)

  def rank(playerId: String): Task[Option[RankResult]] =
    requireId(playerId) *> store.rank(playerId).map(_.map(RankResult(playerId, _)))

  def addAchievement(id: String, name: String): Task[AchievementResult] =
    requireId(id) *> ZIO
      .fail(new IllegalArgumentException("achievement name must not be blank"))
      .when(name.isBlank) *> store.addAchievement(id, name).map(AchievementResult(_))

  def hasAchievement(id: String, name: String): Task[MembershipResult] =
    requireId(id) *> store.hasAchievement(id, name).map(MembershipResult(_))

  def commonAchievements(id1: String, id2: String): Task[List[String]] =
    requireId(id1) *> requireId(id2) *> store.commonAchievements(id1, id2)

  def batchUpsert(inputs: List[(String, PlayerInput)]): Task[List[Player]] =
    for
      _ <- ZIO
        .fail(new IllegalArgumentException("batch must contain between 1 and 100 players"))
        .when(inputs.isEmpty || inputs.size > 100)
      _ <- ZIO.foreachDiscard(inputs) { case (id, input) =>
        requireId(id) *> ZIO
          .fail(new IllegalArgumentException("invalid player"))
          .when(input.name.isBlank || input.region.isBlank || input.level < 0)
      }
      now <- Clock.instant
      players = inputs.map { case (id, input) =>
        Player(id, input.name, input.level, input.region, input.createdAt.getOrElse(now.toString))
      }
      _ <- store.batchUpsert(players)
    yield players

  def consumeNotifications(consumer: String, pollDelay: Duration): Task[Unit] =
    (ZStream.fromZIO(store.ensureNotificationGroup) *>
      ZStream
        .repeatZIO(store.readNotifications(consumer).flatMap { notifications =>
          ZIO.when(notifications.isEmpty)(ZIO.sleep(pollDelay)).as(notifications)
        })
        .mapConcat(identity)).mapZIO { notification =>
      Console.printLine(
        s"notification ${notification.id}: ${notification.playerId} ${notification.kind} ${notification.message}"
      ) *>
        notifications.publish(notification) *>
        store.ackNotification(notification.id)
    }.runDrain
