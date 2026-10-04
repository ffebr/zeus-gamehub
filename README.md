# Zeus 

REST API для игровой платформы GameHub. Стек: Scala 3, ZIO, zio-http, Lettuce,
Redis Sentinel и PureConfig. Модули: `domain`, `config`, `redis`, `service`,
`http`, `app`.

## Запуск

Нужен Docker Compose. Для обычного запуска из корня проекта:

```powershell
docker compose up --build -d
docker compose logs --tail 10 app
```

Демонстрационный скрипт проверяет API, Redis и SSE. Если все контейнеры Zeus
остановлены, он сам выполнит `docker compose up --build -d` и дождётся готовности:

```powershell
./scripts/demo-all.ps1
```

В Bash нужен `curl` и `jq`; аналогичный сценарий:

```bash
bash ./scripts/demo-all.sh
# полный прогон учебного кластера
bash ./scripts/demo-all.sh --with-failover --with-split-brain
```

Если запущена только часть стенда, скрипт покажет недостающие сервисы и остановится.

Скрипт создаёт 20 профилей через batch и двух отдельных игроков, проверяет кэш,
счётчик, рейтинг, достижения, Redis Stream, `XACK` и рассылку двум SSE-клиентам. Для полной
демонстрации инфраструктуры добавьте `-WithFailover -WithSplitBrain`: эти флаги
останавливают текущий мастер и ставят реплики на паузу, затем восстанавливают их.
Запускайте их только на учебном Compose-кластере. Скрипт создаёт новые ID при
каждом запуске и оставляет данные в Redis. Адрес API меняется через `-BaseUrl`.
В Bash для этого используйте `--base-url`.

```powershell
./scripts/demo-all.ps1 -WithFailover -WithSplitBrain
```

На холодном старте ожидание переключения Sentinel ограничено 180 секундами.
При необходимости задайте `-FailoverTimeoutSeconds 300` (для Bash:
`--failover-timeout-seconds 300`).

Отдельная самопроверка домашнего задания использует фиксированные ID:

```powershell
./scripts/seed-demo.ps1
python dz1_check.py
```

API: `http://localhost:8080`. Первый `docker compose up --build` собирает образ;
в запущенном контейнере только JRE и приложение, без sbt. `seed-demo.ps1` загружает
20 профилей и демонстрационные данные. Повторный запуск `seed-demo.ps1` снова увеличит
очки и счётчик входов. Кэш живёт 60 секунд: если проверка его пропустила,
выполните `GET /api/players/1001` и повторите `python dz1_check.py`.

Локально без Docker: `sbt test`, `sbt compile`,
`sbt app/stage`. Проверка стиля:
`sbt scalafmtCheckAll scalafmtSbtCheck "scalafixAll --check"`.

Конфигурация читается PureConfig из
[`application.conf`](app/src/main/resources/application.conf). В ней указаны
внутренние адреса Sentinel; для запуска приложения вне Compose их нужно заменить.
После изменения конфига в Compose выполните `docker compose restart app`.

## API

Insomnia: импортируйте [docs/insomnia-zeus.json](docs/insomnia-zeus.json)
через **Import → File**. Адрес и ID игроков меняются в Base Environment.

| Метод | Путь | JSON запроса | Результат |
|---|---|---|---|
| POST | `/api/players/{id}` | `{"name":"Alice","level":1,"region":"eu"}` | профиль |
| GET | `/api/players/{id}` | — | профиль; 404, если нет |
| PATCH | `/api/players/{id}/level` | `{"delta":2}` | профиль и уведомление |
| POST | `/api/players/{id}/login` | — | `{"count":1}` |
| POST | `/api/leaderboard/score` | `{"playerId":"1001","score":10}` | очки игрока |
| GET | `/api/leaderboard/top?limit=10` | — | список игроков с очками |
| GET | `/api/leaderboard/rank/{playerId}` | — | место (с нуля, как `ZRANK`) |
| POST | `/api/players/{id}/achievements` | `{"name":"first_win"}` | признак добавления |
| GET | `/api/players/{id}/achievements/{name}` | — | признак наличия |
| GET | `/api/players/{id1}/achievements/common/{id2}` | — | общие достижения |
| POST | `/api/players/batch` | массив объектов `{id,name,level,region}` | загруженные профили |
| GET | `/api/notifications/stream` | — | SSE-уведомления в реальном времени |

Статусы: `400` — некорректный запрос, `404` — профиль или место не найдено,
`503` — прочие ошибки сервиса.

Для проверки SSE откройте `curl.exe -N http://localhost:8080/api/notifications/stream`,
затем в другом терминале измените уровень игрока. Событие содержит `id` Redis Stream,
`event` с типом уведомления и JSON в `data`. Подписка получает только новые события:
`Hub` рассылает их подключённым клиентам без истории. Медленный клиент может пропустить
события; Redis Stream при этом остаётся подтверждаемой очередью для фонового consumer.

## Redis

| Данные | Ключ | Тип | TTL |
|---|---|---|---|
| Профиль | `player:{id}` | Hash | — |
| Кэш профиля | `cache:player:{id}` | String | 60 секунд |
| Счётчик входов | `logins:{id}` | String | 24 часа |
| Рейтинг | `tournament:main` | Sorted Set | — |
| Достижения | `achievements:{id}` | Set | — |
| Уведомления | `notifications` | Stream | обрезка старше 7 дней |

Lettuce работает через async API; пакетная загрузка использует pipeline.
Фоновый `ZStream` читает группу `notifications-group`, пишет событие в лог, публикует
его в локальный `Hub` и подтверждает через `XACK`. Pending-записи того же consumer
читаются повторно после перезапуска.

Проверка failover после загрузки демоданных:

```powershell
docker compose stop redis-master
docker compose exec -T sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/players/1001/login
docker compose start redis-master
```

`dz1_check.py` обращается к контейнеру `redis-master`, поэтому запускайте его
до failover. Sentinel хранит свой переписываемый конфиг в томах; изменение
`docker/sentinel-entrypoint.sh` не меняет уже созданные конфиги.
