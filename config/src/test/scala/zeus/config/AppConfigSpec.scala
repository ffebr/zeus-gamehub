package zeus.config

import pureconfig.ConfigSource
import zio.test._

import scala.concurrent.duration._

object AppConfigSpec extends ZIOSpecDefault:
  private val valid = AppConfig(
    RedisConfig(
      List(SentinelAddress("localhost", 26379)),
      "mymaster",
      2.seconds,
      1.second,
      2.seconds
    ),
    ServiceConfig(8080, "zeus-1", 1.second, 2.seconds)
  )

  def spec = suite("AppConfig")(
    test("принимает корректные настройки") {
      assertTrue(valid.validate == Right(valid))
    },
    test("отклоняет пустой список Sentinel и некорректный порт") {
      val noSentinels = valid.copy(redis = valid.redis.copy(sentinels = Nil))
      val badPort =
        valid.copy(redis = valid.redis.copy(sentinels = List(SentinelAddress("localhost", 0))))
      assertTrue(noSentinels.validate.isLeft, badPort.validate.isLeft)
    },
    test("отклоняет некорректный HTTP-порт и неположительные задержки") {
      val badPort = valid.copy(service = valid.service.copy(port = 65536))
      val badPoll = valid.copy(service = valid.service.copy(notificationPollDelay = Duration.Zero))
      val badRetry =
        valid.copy(service = valid.service.copy(notificationRetryDelay = Duration.Zero))
      assertTrue(badPort.validate.isLeft, badPoll.validate.isLeft, badRetry.validate.isLeft)
    },
    test("PureConfig читает вложенную секцию zeus и kebab-case поля") {
      val source = ConfigSource.string("""
        zeus {
          redis {
            sentinels = [{ host = "localhost", port = 26379 }]
            master-id = "mymaster"
            timeout = 2 seconds
            reconnect-delay = 1 second
            startup-retry-delay = 2 seconds
          }
          service {
            port = 8080
            notification-consumer = "zeus-1"
            notification-poll-delay = 1 second
            notification-retry-delay = 2 seconds
          }
        }
      """)
      assertTrue(source.at("zeus").load[AppConfig] == Right(valid))
    }
  )
