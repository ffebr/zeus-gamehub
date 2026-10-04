package zeus.config

import pureconfig._
import pureconfig.generic.derivation.default._

import scala.concurrent.duration.FiniteDuration

final case class SentinelAddress(host: String, port: Int) derives ConfigReader

final case class RedisConfig(
    sentinels: List[SentinelAddress],
    masterId: String,
    timeout: FiniteDuration,
    reconnectDelay: FiniteDuration,
    startupRetryDelay: FiniteDuration
) derives ConfigReader

final case class ServiceConfig(
    port: Int,
    notificationConsumer: String,
    notificationPollDelay: FiniteDuration,
    notificationRetryDelay: FiniteDuration
) derives ConfigReader

final case class AppConfig(redis: RedisConfig, service: ServiceConfig) derives ConfigReader:
  def validate: Either[String, AppConfig] =
    val invalid =
      if redis.masterId.isBlank then Some("zeus.redis.master-id must not be blank")
      else if redis.sentinels.isEmpty then Some("zeus.redis.sentinels must not be empty")
      else if redis.sentinels.exists(s => s.host.isBlank || s.port < 1 || s.port > 65535) then
        Some("zeus.redis.sentinels must contain valid host and port values")
      else if redis.timeout.length <= 0 || redis.reconnectDelay.length <= 0 || redis.startupRetryDelay.length <= 0
      then Some("Redis timeouts and retry delays must be positive")
      else if service.port < 1 || service.port > 65535 then
        Some("zeus.service.port must be between 1 and 65535")
      else if service.notificationConsumer.isBlank then
        Some("zeus.service.notification-consumer must not be blank")
      else if service.notificationPollDelay.length <= 0 then
        Some("notification poll delay must be positive")
      else if service.notificationRetryDelay.length <= 0 then
        Some("notification retry delay must be positive")
      else None
    invalid.toLeft(this)

object AppConfig:
  def load: Either[String, AppConfig] =
    ConfigSource.default
      .at("zeus")
      .load[AppConfig]
      .left
      .map(_.prettyPrint())
      .flatMap(_.validate)
