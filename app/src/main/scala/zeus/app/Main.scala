package zeus.app

import zeus.config.AppConfig
import zeus.domain.GameStore
import zeus.domain.Notification
import zeus.http.GameRoutes
import zeus.redis.RedisGameStore
import zeus.service.GameService
import zio._
import zio.http.Server

object Main extends ZIOAppDefault:
  override def run: ZIO[Any, Throwable, Unit] =
    for
      config <- ZIO.fromEither(AppConfig.load.left.map(new IllegalArgumentException(_)))
      _ <- ZIO
        .scoped {
          for
            store <- ZIO.service[GameStore]
            _ <- store.ensureNotificationGroup
              .tapError(e =>
                ZIO.logWarning(s"notification group startup retrying: ${e.getMessage}")
              )
              .retry(Schedule.spaced(zio.Duration.fromScala(config.redis.startupRetryDelay)))
            notifications <- Hub.sliding[Notification](256)
            service = new GameService(store, notifications)
            _ <- service
              .consumeNotifications(
                config.service.notificationConsumer,
                zio.Duration.fromScala(config.service.notificationPollDelay)
              )
              .tapError(e => ZIO.logWarning(s"notification consumer retrying: ${e.getMessage}"))
              .retry(Schedule.spaced(zio.Duration.fromScala(config.service.notificationRetryDelay)))
              .forkScoped
            _ <- ZIO.logInfo(s"Zeus listening on ${config.service.port}")
            _ <- Server.serve(GameRoutes.routes(service))
          yield ()
        }
        .provide(
          RedisGameStore
            .layer(config.redis)
            .retry(Schedule.spaced(zio.Duration.fromScala(config.redis.startupRetryDelay))),
          Server.defaultWithPort(config.service.port)
        )
    yield ()
