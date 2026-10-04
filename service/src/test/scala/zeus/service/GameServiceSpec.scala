package zeus.service

import zeus.domain._
import zio._
import zio.test._

object GameServiceSpec extends ZIOSpecDefault:
  private val input = PlayerInput("Alice", 2L, "eu", Some("2024-01-01T00:00:00Z"))

  private final case class StoreState(
      players: Map[String, Player] = Map.empty,
      notifications: Vector[Notification] = Vector.empty,
      logins: Map[String, Long] = Map.empty,
      scores: Map[String, Double] = Map.empty,
      achievements: Map[String, Set[String]] = Map.empty,
      groupReady: Boolean = false,
      acknowledged: Vector[String] = Vector.empty
  )

  private final class InMemoryStore(state: Ref[StoreState], ackSignal: Promise[Nothing, Unit])
      extends GameStore:
    def snapshot: UIO[StoreState] = state.get
    def awaitAck: UIO[Unit] = ackSignal.await

    def upsertPlayer(player: Player): Task[Unit] =
      state.update(s => s.copy(players = s.players.updated(player.id, player)))

    def getPlayer(id: String): Task[Option[Player]] = state.get.map(_.players.get(id))

    def incrementLevel(id: String, delta: Long): Task[Option[Long]] =
      state.modify { s =>
        s.players.get(id) match
          case None         => (None, s)
          case Some(player) =>
            val level = player.level + delta
            (Some(level), s.copy(players = s.players.updated(id, player.copy(level = level))))
      }

    def recordLogin(id: String): Task[Long] =
      state.modify { s =>
        val count = s.logins.getOrElse(id, 0L) + 1L
        (count, s.copy(logins = s.logins.updated(id, count)))
      }

    def addScore(playerId: String, score: Double): Task[Double] =
      state.modify { s =>
        val total = s.scores.getOrElse(playerId, 0.0) + score
        (total, s.copy(scores = s.scores.updated(playerId, total)))
      }

    def top(limit: Int): Task[List[LeaderboardEntry]] =
      state.get.map(
        _.scores.toList
          .sortBy { case (_, score) => -score }
          .take(limit)
          .map { case (id, score) => LeaderboardEntry(id, score) }
      )

    def rank(playerId: String): Task[Option[Long]] =
      state.get.map { s =>
        val index =
          s.scores.toList.sortBy { case (_, score) => -score }.indexWhere(_._1 == playerId)
        Option.when(index >= 0)(index.toLong)
      }

    def addAchievement(id: String, name: String): Task[Boolean] =
      state.modify { s =>
        val before = s.achievements.getOrElse(id, Set.empty[String])
        (!before.contains(name), s.copy(achievements = s.achievements.updated(id, before + name)))
      }

    def hasAchievement(id: String, name: String): Task[Boolean] =
      state.get.map(_.achievements.getOrElse(id, Set.empty).contains(name))

    def commonAchievements(id1: String, id2: String): Task[List[String]] =
      state.get.map { s =>
        (s.achievements.getOrElse(id1, Set.empty) intersect s.achievements.getOrElse(
          id2,
          Set.empty
        )).toList.sorted
      }

    def batchUpsert(players: List[Player]): Task[Unit] =
      state.update(s => s.copy(players = s.players ++ players.map(p => p.id -> p)))

    def ensureNotificationGroup: Task[Unit] = state.update(_.copy(groupReady = true))
    def publishNotification(notification: Notification): Task[Unit] =
      state.update(s => s.copy(notifications = s.notifications :+ notification))
    def readNotifications(consumer: String): Task[List[Notification]] =
      state.modify(s => (s.notifications.toList, s.copy(notifications = Vector.empty)))
    def ackNotification(id: String): Task[Unit] =
      state.update(s => s.copy(acknowledged = s.acknowledged :+ id)) *> ackSignal.succeed(()).unit

  private object InMemoryStore:
    def make: UIO[InMemoryStore] =
      for
        state <- Ref.make(StoreState())
        signal <- Promise.make[Nothing, Unit]
      yield new InMemoryStore(state, signal)

  private def makeService(store: GameStore): UIO[GameService] =
    Hub.sliding[Notification](256).map(new GameService(store, _))

  def spec = suite("GameService")(
    test("создаёт профиль без уведомления") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        player <- service.upsertPlayer("1001", input)
        saved <- store.getPlayer("1001")
        snapshot <- store.snapshot
      yield assertTrue(
        player == Player("1001", "Alice", 2L, "eu", "2024-01-01T00:00:00Z"),
        saved.contains(player),
        snapshot.notifications.isEmpty
      )
    },
    test("обновление сохраняет createdAt и отправляет уведомление") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        _ <- service.upsertPlayer("1001", input)
        updated <- service.upsertPlayer("1001", PlayerInput("Alice 2", 3L, "us", Some("ignored")))
        snapshot <- store.snapshot
      yield assertTrue(
        updated.createdAt == "2024-01-01T00:00:00Z",
        updated.name == "Alice 2",
        snapshot.notifications.map(_.kind) == Vector("profile_updated")
      )
    },
    test("изменение уровня возвращает профиль и создаёт уведомление") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        _ <- service.upsertPlayer("1001", input)
        changed <- service.changeLevel("1001", 5L)
        snapshot <- store.snapshot
      yield assertTrue(
        changed.exists(_.level == 7L),
        snapshot.notifications.map(_.kind) == Vector("level_changed"),
        snapshot.notifications.head.playerId == "1001"
      )
    },
    test("для отсутствующего профиля изменение уровня не создаёт уведомление") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        changed <- service.changeLevel("missing", 1L)
        snapshot <- store.snapshot
      yield assertTrue(changed.isEmpty, snapshot.notifications.isEmpty)
    },
    test("отклоняет некорректные входные данные до записи") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        blankId <- service.getPlayer(" ").either
        badPlayer <- service.upsertPlayer("1001", input.copy(level = -1L)).either
        badScore <- service.addScore(ScoreInput("1001", Double.NaN)).either
        badLimit <- service.top(101).either
        badAchievement <- service.addAchievement("1001", " ").either
        snapshot <- store.snapshot
      yield assertTrue(
        blankId.isLeft,
        badPlayer.isLeft,
        badScore.isLeft,
        badLimit.isLeft,
        badAchievement.isLeft,
        snapshot.players.isEmpty,
        snapshot.scores.isEmpty
      )
    },
    test("пакетная запись валидирует размер и сохраняет все профили") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        empty <- service.batchUpsert(Nil).either
        tooLarge <- service.batchUpsert(List.fill(101)("id" -> input)).either
        created <- service.batchUpsert(List("1001" -> input, "1002" -> input.copy(name = "Bob")))
        snapshot <- store.snapshot
      yield assertTrue(
        empty.isLeft,
        tooLarge.isLeft,
        created.map(_.id) == List("1001", "1002"),
        snapshot.players.keySet == Set("1001", "1002")
      )
    },
    test("счётчик входов, рейтинг и достижения проходят через сервис") {
      for
        store <- InMemoryStore.make
        service <- makeService(store)
        firstLogin <- service.login("1001")
        secondLogin <- service.login("1001")
        _ <- service.addScore(ScoreInput("1001", 10.0))
        total <- service.addScore(ScoreInput("1001", 5.0))
        _ <- service.addScore(ScoreInput("1002", 20.0))
        rank <- service.rank("1001")
        leaders <- service.top(2)
        firstAchievement <- service.addAchievement("1001", "first_win")
        duplicateAchievement <- service.addAchievement("1001", "first_win")
        _ <- service.addAchievement("1002", "first_win")
        common <- service.commonAchievements("1001", "1002")
      yield assertTrue(
        firstLogin.count == 1L,
        secondLogin.count == 2L,
        total.score == 15.0,
        rank.exists(_.rank == 1L),
        leaders.map(_.playerId) == List("1002", "1001"),
        firstAchievement.added,
        !duplicateAchievement.added,
        common == List("first_win")
      )
    },
    test("потребитель потока печатает и подтверждает уведомление") {
      val notification =
        Notification("1-0", "1001", "level_changed", "Level changed", "2024-01-01T00:00:00Z")
      for
        store <- InMemoryStore.make
        _ <- store.publishNotification(notification)
        hub <- Hub.sliding[Notification](256)
        service = new GameService(store, hub)
        result <- ZIO.scoped {
          for
            first <- hub.subscribe
            second <- hub.subscribe
            fiber <- service.consumeNotifications("test-consumer", 1.second).fork
            _ <- store.awaitAck
            delivered1 <- first.take
            delivered2 <- second.take
            _ <- fiber.interrupt
            snapshot <- store.snapshot
            output <- TestConsole.output
          yield (delivered1, delivered2, snapshot, output)
        }
        (delivered1, delivered2, snapshot, output) = result
      yield assertTrue(
        snapshot.groupReady,
        snapshot.acknowledged == Vector("1-0"),
        delivered1 == notification,
        delivered2 == notification,
        output.exists(_.contains("notification 1-0: 1001 level_changed Level changed"))
      )
    }
  )
