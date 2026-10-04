package zeus.redis

import io.lettuce.core.Consumer
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.XGroupCreateArgs
import io.lettuce.core.XReadArgs
import io.lettuce.core.XTrimArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.resource.Delay
import zeus.config.RedisConfig
import zeus.domain._
import zio._
import zio.json._

import java.time.Duration
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

object RedisGameStore:
  private val LoginScript =
    "local n = redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], 86400); return n"
  private val CacheTtlSeconds = 60L
  private val StreamRetentionMillis = 7L * 24 * 60 * 60 * 1000
  private val StreamKey = "notifications"
  private val Group = "notifications-group"

  def layer(config: RedisConfig): ZLayer[Any, Throwable, GameStore] =
    ZLayer.scoped {
      for
        resources <- ZIO.acquireRelease(
          ZIO.attempt(
            DefaultClientResources
              .builder()
              .reconnectDelay(Delay.constant(Duration.ofMillis(config.reconnectDelay.toMillis)))
              .build()
          )
        )(r => ZIO.attempt(r.shutdown()).orDie)
        uri <- ZIO.attempt(makeUri(config))
        client <- ZIO.acquireRelease(ZIO.attempt(makeClient(uri, resources)))(c =>
          ZIO.fromCompletionStage(c.shutdownAsync()).orDie
        )
        connection <- ZIO.acquireRelease(
          ZIO.fromCompletionStage(client.connectAsync(StringCodec.UTF8, uri))
        )(c => ZIO.fromCompletionStage(c.closeAsync()).orDie)
        _ <- ZIO.fromCompletionStage(connection.async().ping())
      yield new Live(client, uri, connection)
    }

  private def makeUri(config: RedisConfig): RedisURI =
    require(config.sentinels.nonEmpty, "at least one Redis Sentinel is required")
    val builder = RedisURI
      .builder()
      .withSentinelMasterId(config.masterId)
      .withTimeout(Duration.ofMillis(config.timeout.toMillis))
    config.sentinels.foreach(s => builder.withSentinel(s.host, s.port))
    builder.build()

  private def makeClient(uri: RedisURI, resources: DefaultClientResources): RedisClient =
    val client = RedisClient.create(resources, uri)
    client.setOptions(
      io.lettuce.core.ClientOptions
        .builder()
        .autoReconnect(true)
        .disconnectedBehavior(io.lettuce.core.ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .build()
    )
    client

  private final class Live(
      client: RedisClient,
      uri: RedisURI,
      connection: StatefulRedisConnection[String, String]
  ) extends GameStore:
    private def commands: RedisAsyncCommands[String, String] = connection.async()
    private def playerKey(id: String) = s"player:$id"
    private def cacheKey(id: String) = s"cache:player:$id"
    private def achievementKey(id: String) = s"achievements:$id"

    private def execute[A](command: => RedisFuture[A]): Task[A] =
      ZIO.fromCompletionStage(command)

    private def decodePlayer(id: String, fields: java.util.Map[String, String]): Option[Player] =
      if fields == null || fields.isEmpty then None
      else
        Option(fields.get("name")).map { name =>
          Player(
            id,
            name,
            Option(fields.get("level")).fold(0L)(_.toLong),
            fields.getOrDefault("region", ""),
            fields.getOrDefault("created_at", "")
          )
        }

    def upsertPlayer(player: Player): Task[Unit] =
      execute(
        commands.hset(
          playerKey(player.id),
          Map(
            "name" -> player.name,
            "level" -> player.level.toString,
            "region" -> player.region,
            "created_at" -> player.createdAt
          ).asJava
        )
      ) *> execute(commands.del(cacheKey(player.id))).unit

    def getPlayer(id: String): Task[Option[Player]] =
      execute(commands.get(cacheKey(id))).flatMap { cached =>
        Option(cached).flatMap(_.fromJson[Player].toOption) match
          case hit @ Some(_) => ZIO.succeed(hit)
          case None          =>
            execute(commands.hgetall(playerKey(id))).map(decodePlayer(id, _)).flatMap { profile =>
              ZIO
                .foreachDiscard(profile)(p =>
                  execute(commands.setex(cacheKey(id), CacheTtlSeconds, p.toJson))
                )
                .as(profile)
            }
      }

    def incrementLevel(id: String, delta: Long): Task[Option[Long]] =
      execute(commands.exists(playerKey(id))).flatMap { exists =>
        if exists == 0 then ZIO.none
        else
          execute(commands.hincrby(playerKey(id), "level", delta)).flatMap { level =>
            execute(commands.del(cacheKey(id))).as(Some(level.longValue()))
          }
      }

    def recordLogin(id: String): Task[Long] =
      execute(
        commands.eval[java.lang.Long](LoginScript, ScriptOutputType.INTEGER, Array(s"logins:$id"))
      )
        .map(_.longValue())

    def addScore(playerId: String, score: Double): Task[Double] =
      execute(commands.zincrby("tournament:main", score, playerId)).map(_.doubleValue())

    def top(limit: Int): Task[List[LeaderboardEntry]] =
      execute(commands.zrevrangeWithScores("tournament:main", 0L, limit.toLong - 1L)).map(
        _.asScala.toList
          .map(kv => LeaderboardEntry(kv.getValue, kv.getScore))
      )

    def rank(playerId: String): Task[Option[Long]] =
      execute(commands.zrank("tournament:main", playerId)).map(value =>
        Option(value).map(_.longValue())
      )

    def addAchievement(id: String, name: String): Task[Boolean] =
      execute(commands.sadd(achievementKey(id), name)).map(_ == 1L)
    def hasAchievement(id: String, name: String): Task[Boolean] =
      execute(commands.sismember(achievementKey(id), name)).map(_.booleanValue())
    def commonAchievements(id1: String, id2: String): Task[List[String]] =
      execute(commands.sinter(achievementKey(id1), achievementKey(id2))).map(_.asScala.toList)

    def batchUpsert(players: List[Player]): Task[Unit] =
      ZIO.scoped {
        for
          batchConnection <- ZIO.acquireRelease(
            ZIO.fromCompletionStage(client.connectAsync(StringCodec.UTF8, uri))
          )(c => ZIO.fromCompletionStage(c.closeAsync()).orDie)
          futures <- ZIO.attempt {
            val async = batchConnection.async()
            batchConnection.setAutoFlushCommands(false)
            val pending: List[RedisFuture[?]] = players.flatMap { p =>
              val fields = Map(
                "name" -> p.name,
                "level" -> p.level.toString,
                "region" -> p.region,
                "created_at" -> p.createdAt
              ).asJava
              List(async.hset(playerKey(p.id), fields), async.del(cacheKey(p.id)))
            }
            batchConnection.flushCommands()
            pending
          }
          _ <- ZIO.fromCompletionStage(
            CompletableFuture.allOf(futures.map(_.toCompletableFuture).toArray*)
          )
        yield ()
      }

    def ensureNotificationGroup: Task[Unit] =
      execute(
        commands.xgroupCreate(
          XReadArgs.StreamOffset.from(StreamKey, "$"),
          Group,
          XGroupCreateArgs.Builder.mkstream()
        )
      ).unit.catchSome {
        case e: io.lettuce.core.RedisCommandExecutionException
            if e.getMessage.contains("BUSYGROUP") =>
          ZIO.unit
      }

    def publishNotification(notification: Notification): Task[Unit] =
      execute(
        commands.xadd(
          StreamKey,
          Map(
            "player_id" -> notification.playerId,
            "type" -> notification.kind,
            "message" -> notification.message,
            "timestamp" -> notification.timestamp
          ).asJava
        )
      ) *> ZIO
        .succeed(java.lang.System.currentTimeMillis() - StreamRetentionMillis)
        .flatMap(cutoff =>
          execute(commands.xtrim(StreamKey, XTrimArgs.Builder.minId(s"$cutoff-0"))).unit
        )

    def readNotifications(consumer: String): Task[List[Notification]] =
      val reader = Consumer.from(Group, consumer)
      val args = XReadArgs.Builder.count(100)
      execute(commands.xreadgroup(reader, args, XReadArgs.StreamOffset.from(StreamKey, "0")))
        .flatMap { pending =>
          if pending.isEmpty then
            execute(
              commands.xreadgroup(reader, args, XReadArgs.StreamOffset.lastConsumed(StreamKey))
            )
          else ZIO.succeed(pending)
        }
        .map(_.asScala.toList.flatMap { message =>
          val body = message.getBody
          Option(body.get("player_id")).map(id =>
            Notification(
              message.getId,
              id,
              body.getOrDefault("type", ""),
              body.getOrDefault("message", ""),
              body.getOrDefault("timestamp", "")
            )
          )
        })

    def ackNotification(id: String): Task[Unit] = execute(commands.xack(StreamKey, Group, id)).unit
