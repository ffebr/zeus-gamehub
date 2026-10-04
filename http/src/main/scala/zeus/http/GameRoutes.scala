package zeus.http

import zeus.domain._
import zeus.service.GameService
import zio._
import zio.http._
import zio.json._

/** REST endpoints for the GameHub service. HTTP code intentionally depends only on the service
  * layer.
  */
object GameRoutes:
  private final case class BatchPlayer(
      id: String,
      name: String,
      level: Long,
      region: String,
      createdAt: Option[String] = None
  )
  private object BatchPlayer:
    given JsonCodec[BatchPlayer] = DeriveJsonCodec.gen[BatchPlayer]

  private def json[A: JsonEncoder](value: A): Response = Response.json(value.toJson)
  private def error(status: Status, message: String): Response =
    Response.json(Map("error" -> message).toJson).status(status)

  private def protect[A](task: Task[A])(render: A => Response): IO[Response, Response] =
    task.foldZIO(
      {
        case e: IllegalArgumentException =>
          ZIO.fail(error(Status.BadRequest, Option(e.getMessage).getOrElse("invalid request")))
        case e =>
          ZIO.fail(
            error(Status.ServiceUnavailable, Option(e.getMessage).getOrElse("service unavailable"))
          )
      },
      value => ZIO.succeed(render(value))
    )

  private def decode[A: JsonDecoder](request: Request)(
      use: A => IO[Response, Response]
  ): IO[Response, Response] =
    request.body.asString.foldZIO(
      _ => ZIO.fail(error(Status.BadRequest, "invalid request body")),
      raw =>
        raw.fromJson[A] match
          case Left(message) => ZIO.fail(error(Status.BadRequest, message))
          case Right(value)  => use(value)
    )

  def routes(service: GameService): Routes[Any, Response] =
    Routes(
      Method.GET / "api" / "notifications" / "stream" -> handler {
        Response.fromServerSentEvents(
          service.notificationStream.map(notification =>
            ServerSentEvent(
              notification.toJson,
              Some(notification.kind),
              Some(notification.id),
              None
            )
          )
        )
      },
      Method.POST / "api" / "players" / "batch" -> handler { (request: Request) =>
        decode[List[BatchPlayer]](request) { entries =>
          val inputs = entries.map(p => p.id -> PlayerInput(p.name, p.level, p.region, p.createdAt))
          protect(service.batchUpsert(inputs))(json(_))
        }
      },
      Method.POST / "api" / "players" / string("id") -> handler { (id: String, request: Request) =>
        decode[PlayerInput](request)(input => protect(service.upsertPlayer(id, input))(json(_)))
      },
      Method.GET / "api" / "players" / string("id") -> handler { (id: String, _: Request) =>
        protect(service.getPlayer(id)) {
          case Some(player) => json(player)
          case None         => error(Status.NotFound, "player not found")
        }
      },
      Method.PATCH / "api" / "players" / string("id") / "level" -> handler {
        (id: String, request: Request) =>
          decode[LevelInput](request)(input =>
            protect(service.changeLevel(id, input.delta)) {
              case Some(player) => json(player)
              case None         => error(Status.NotFound, "player not found")
            }
          )
      },
      Method.POST / "api" / "players" / string("id") / "login" -> handler {
        (id: String, _: Request) =>
          protect(service.login(id))(json(_))
      },
      Method.POST / "api" / "leaderboard" / "score" -> handler { (request: Request) =>
        decode[ScoreInput](request)(input => protect(service.addScore(input))(json(_)))
      },
      Method.GET / "api" / "leaderboard" / "top" -> handler { (request: Request) =>
        val limit =
          request.url.queryParams.getAll("limit").headOption.flatMap(_.toIntOption).getOrElse(10)
        protect(service.top(limit))(json(_))
      },
      Method.GET / "api" / "leaderboard" / "rank" / string("playerId") -> handler {
        (playerId: String, _: Request) =>
          protect(service.rank(playerId)) {
            case Some(rank) => json(rank)
            case None       => error(Status.NotFound, "player is not ranked")
          }
      },
      Method.POST / "api" / "players" / string("id") / "achievements" -> handler {
        (id: String, request: Request) =>
          decode[AchievementInput](request)(input =>
            protect(service.addAchievement(id, input.name))(json(_))
          )
      },
      Method.GET / "api" / "players" / string("id") / "achievements" / string("name") -> handler {
        (id: String, name: String, _: Request) =>
          protect(service.hasAchievement(id, name))(json(_))
      },
      Method.GET / "api" / "players" / string("id1") / "achievements" / "common" / string(
        "id2"
      ) -> handler { (id1: String, id2: String, _: Request) =>
        protect(service.commonAchievements(id1, id2))(json(_))
      }
    )
