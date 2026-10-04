param(
    [string]$BaseUrl = 'http://localhost:8080',
    [switch]$WithFailover,
    [switch]$WithSplitBrain,
    [ValidateRange(1, 3600)][int]$FailoverTimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$BaseUrl = $BaseUrl.TrimEnd('/')
$redisServices = @('redis-master', 'redis-replica-1', 'redis-replica-2')
$runId = (New-Guid).ToString('N').Substring(0, 8)
$playerA = "demo-$runId-a"
$playerB = "demo-$runId-b"

function Show-Step([string]$Title) {
    Write-Host "`n=== $Title ===" -ForegroundColor Cyan
}

function Assert-Demo([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "FAIL: $Message" }
    Write-Host "OK: $Message" -ForegroundColor Green
}

function Invoke-Api([string]$Method, [string]$Path, $Body) {
    $request = @{
        Method = $Method
        Uri = "$BaseUrl$Path"
        ErrorAction = 'Stop'
    }
    if ($PSBoundParameters.ContainsKey('Body')) {
        $request.ContentType = 'application/json'
        $request.Body = ConvertTo-Json -InputObject $Body -Depth 8 -Compress
    }
    Invoke-RestMethod @request
}

function Get-ApiStatus([string]$Method, [string]$Path, $Body) {
    $request = @{
        Method = $Method
        Uri = "$BaseUrl$Path"
        UseBasicParsing = $true
        ErrorAction = 'Stop'
    }
    if ($PSBoundParameters.ContainsKey('Body')) {
        $request.ContentType = 'application/json'
        $request.Body = ConvertTo-Json -InputObject $Body -Depth 8 -Compress
    }
    try {
        $response = Invoke-WebRequest @request
        return [int]$response.StatusCode
    } catch {
        if ($null -eq $_.Exception.Response) { throw }
        return [int]$_.Exception.Response.StatusCode
    }
}

function Invoke-Redis([string]$Service, [string[]]$RedisArgs, [int]$Port = 6379) {
    $result = @(& docker compose exec -T $Service redis-cli -p $Port --raw @RedisArgs 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "redis-cli ($Service): $($result -join ' ')" }
    return $result
}

function Get-MasterIp {
    $address = @(Invoke-Redis 'sentinel-1' @('SENTINEL', 'get-master-addr-by-name', 'mymaster') 26379)
    if ($address.Count -lt 2 -or -not $address[0]) { throw 'Sentinel не сообщил адрес мастера' }
    return [string]$address[0]
}

function Wait-ForMasterIp([int]$Seconds = 90) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
        try { return Get-MasterIp } catch { Start-Sleep -Seconds 2 }
    } while ((Get-Date) -lt $deadline)
    throw 'Sentinel не обнаружил мастер за отведённое время'
}

function Wait-ForApi([int]$Seconds = 90) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
        try {
            if ((Get-ApiStatus 'GET' "/api/players/demo-$runId-healthcheck") -eq 404) { return $true }
        } catch { }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    return $false
}

function Get-MasterService([string]$Address) {
    foreach ($service in $redisServices) {
        $containerId = @(& docker compose ps -q $service) | Select-Object -First 1
        if (-not $containerId) { continue }
        $ip = @(& docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $containerId) |
            Select-Object -First 1
        if ([string]$ip -eq $Address) { return $service }
    }
    throw "Не удалось сопоставить адрес мастера $Address с сервисом Compose"
}

function Get-RedisValue([string]$Service, [string[]]$RedisArgs) {
    $lines = @(Invoke-Redis $Service $RedisArgs)
    return [string]($lines | Select-Object -First 1)
}

function Get-RedisConfig([string]$Service, [string]$Name) {
    $lines = @(Invoke-Redis $Service @('CONFIG', 'GET', $Name))
    if ($lines.Count -lt 2) { throw "Redis не вернул настройку $Name" }
    return [string]$lines[1]
}

function Restore-ReplicaRuntimeConfig([string]$Service) {
    if ($Service -eq 'redis-master') { return }
    $settings = @(
        @('appendonly', 'yes'),
        @('maxmemory', '268435456'),
        @('maxmemory-policy', 'volatile-lru')
    )
    foreach ($setting in $settings) {
        $result = Get-RedisValue $Service @('CONFIG', 'SET', $setting[0], $setting[1])
        if ($result -ne 'OK') { throw "Не удалось восстановить настройку $($setting[0]) на $Service`: $result" }
    }
    Write-Host "Настройки AOF и памяти восстановлены на $Service"
}

function Wait-ForTwoReplicas([string]$Service, [int]$Seconds = 60) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
        $replication = @(Invoke-Redis $Service @('INFO', 'replication'))
        if ($replication -contains 'connected_slaves:2') { return $true }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    return $false
}

function Wait-ForNoGoodReplicas([string]$Service, [int]$Seconds = 60) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
        $replication = @(Invoke-Redis $Service @('INFO', 'replication'))
        if ($replication -contains 'min_slaves_good_slaves:0') { return $true }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    return $false
}

function Start-SseReader([string]$PlayerId) {
    Start-Job -ArgumentList "$BaseUrl/api/notifications/stream", $PlayerId -ScriptBlock {
        param($Url, $ExpectedPlayerId)
        $request = [System.Net.WebRequest]::Create($Url)
        $request.Accept = 'text/event-stream'
        $request.Timeout = 15000
        $request.ReadWriteTimeout = 15000
        $response = $null
        $reader = $null
        try {
            $response = $request.GetResponse()
            if (-not $response.ContentType.StartsWith('text/event-stream')) {
                throw "Неверный Content-Type: $($response.ContentType)"
            }
            $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
            Write-Output 'CONNECTED'
            $kind = ''
            $id = ''
            $data = ''
            while ($true) {
                $line = $reader.ReadLine()
                if ($null -eq $line) { throw 'SSE-соединение закрылось до уведомления' }
                if ($line -eq '') {
                    if ($data) {
                        $payload = $data | ConvertFrom-Json
                        if ($payload.playerId -eq $ExpectedPlayerId -and $payload.kind -eq 'level_changed') {
                            Write-Output ([pscustomobject]@{ Kind = $kind; Id = $id; Payload = $payload })
                            break
                        }
                    }
                    $kind = ''
                    $id = ''
                    $data = ''
                } elseif ($line.StartsWith('event:')) {
                    $kind = $line.Substring(6).Trim()
                } elseif ($line.StartsWith('id:')) {
                    $id = $line.Substring(3).Trim()
                } elseif ($line.StartsWith('data:')) {
                    $data = $line.Substring(5).Trim()
                }
            }
        } finally {
            if ($reader) { $reader.Dispose() }
            if ($response) { $response.Dispose() }
            $request.Abort()
        }
    }
}

function Wait-SseConnected($Jobs) {
    foreach ($job in $Jobs) {
        $deadline = (Get-Date).AddSeconds(15)
        while (@(Receive-Job $job -Keep) -notcontains 'CONNECTED') {
            if ($job.State -ne 'Running' -or (Get-Date) -gt $deadline) {
                throw "SSE-клиент не подключился: $($job.State)"
            }
            Start-Sleep -Milliseconds 100
        }
    }
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    Show-Step '1. Docker, репликация и Sentinel'
    $required = @($redisServices) + @('sentinel-1', 'sentinel-2', 'sentinel-3', 'app')
    $running = @(& docker compose ps --status running --services | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось получить состояние Docker Compose' }
    if ($running.Count -eq 0) {
        Write-Host 'Стенд не запущен. Выполняю docker compose up --build -d...'
        & docker compose up --build --quiet-build --quiet-pull -d | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Не удалось запустить Docker Compose' }
        $running = @(& docker compose ps --status running --services | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    }
    $missingServices = @($required | Where-Object { $running -notcontains $_ })
    if ($missingServices.Count -gt 0) {
        throw "Запущена только часть стенда. Не работают: $($missingServices -join ', '). Проверьте docker compose ps -a и выполните docker compose up --build -d."
    }
    foreach ($service in $required) {
        Assert-Demo ($running -contains $service) "$service запущен"
    }
    $masterIp = Wait-ForMasterIp
    $masterService = Get-MasterService $masterIp
    $replication = @(Invoke-Redis $masterService @('INFO', 'replication'))
    Assert-Demo ($replication -contains 'role:master') "Sentinel выбрал $masterService ($masterIp)"
    Assert-Demo (Wait-ForTwoReplicas $masterService) 'к мастеру подключены две реплики'
    Assert-Demo (Wait-ForApi) 'API отвечает на запросы'
    $aof = Get-RedisConfig $masterService 'appendonly'
    $fsync = Get-RedisConfig $masterService 'appendfsync'
    $rdbSave = Get-RedisConfig $masterService 'save'
    $eviction = Get-RedisConfig $masterService 'maxmemory-policy'
    $maxMemory = [long](Get-RedisConfig $masterService 'maxmemory')
    $minReplicas = [int](Get-RedisConfig $masterService 'min-replicas-to-write')
    Assert-Demo ($aof -eq 'yes' -and $fsync -eq 'everysec' -and -not [string]::IsNullOrWhiteSpace($rdbSave) -and $eviction -eq 'volatile-lru' -and $maxMemory -gt 0 -and $minReplicas -ge 1) 'AOF/RDB, лимит памяти, volatile-lru и min-replicas-to-write настроены'
    Write-Host "Redis: $masterService; Sentinel: $masterIp"

    Show-Step '2. Профили, pipeline и кэш'
    $batch = @(1..20 | ForEach-Object {
        @{ id = "demo-$runId-batch-$_"; name = "Batch $_"; level = 1; region = 'eu' }
    })
    $created = Invoke-Api 'POST' '/api/players/batch' $batch
    Assert-Demo ($created.Count -eq 20) '20 профилей загружены одним batch-запросом (pipeline)'
    $profile = Invoke-Api 'POST' "/api/players/$playerA" @{ name = 'Alice'; level = 2; region = 'eu' }
    Assert-Demo ($profile.id -eq $playerA) 'профиль создан'
    Write-Host "Профиль: $(ConvertTo-Json -InputObject $profile -Compress)"
    Assert-Demo ((Get-RedisValue $masterService @('TYPE', "player:$playerA")) -eq 'hash') 'профиль хранится как Redis Hash'
    $null = Invoke-Api 'POST' "/api/players/$playerB" @{ name = 'Bob'; level = 1; region = 'us' }
    $first = Invoke-Api 'GET' "/api/players/$playerA"
    $ttl1 = [int](Get-RedisValue $masterService @('TTL', "cache:player:$playerA"))
    Start-Sleep -Seconds 2
    $second = Invoke-Api 'GET' "/api/players/$playerA"
    $ttl2 = [int](Get-RedisValue $masterService @('TTL', "cache:player:$playerA"))
    Assert-Demo ($first.id -eq $second.id -and $ttl1 -gt 0 -and $ttl2 -gt 0 -and $ttl2 -lt $ttl1) "кэш заполнился и повторный GET не обновил TTL ($ttl1 -> $ttl2 с)"
    $updated = Invoke-Api 'POST' "/api/players/$playerA" @{ name = 'Alice Updated'; level = 2; region = 'eu' }
    $invalidatedTtl = [int](Get-RedisValue $masterService @('TTL', "cache:player:$playerA"))
    Assert-Demo ($updated.createdAt -eq $profile.createdAt -and $invalidatedTtl -eq -2) 'обновление сохранило createdAt и сбросило кэш'
    $null = Invoke-Api 'GET' "/api/players/$playerA"

    Show-Step '3. Входы, рейтинг и достижения'
    $login1 = Invoke-Api 'POST' "/api/players/$playerA/login"
    $login2 = Invoke-Api 'POST' "/api/players/$playerA/login"
    $loginTtl = [int](Get-RedisValue $masterService @('TTL', "logins:$playerA"))
    $loginType = Get-RedisValue $masterService @('TYPE', "logins:$playerA")
    Assert-Demo ($login1.count -eq 1 -and $login2.count -eq 2 -and $loginTtl -gt 0 -and $loginType -eq 'string') "два входа, Redis String = 2, TTL $loginTtl с"
    $scoreA = Invoke-Api 'POST' '/api/leaderboard/score' @{ playerId = $playerA; score = 120 }
    $scoreB = Invoke-Api 'POST' '/api/leaderboard/score' @{ playerId = $playerB; score = 80 }
    $scoreA2 = Invoke-Api 'POST' '/api/leaderboard/score' @{ playerId = $playerA; score = 5 }
    $top = Invoke-Api 'GET' '/api/leaderboard/top?limit=10'
    $rank = Invoke-Api 'GET' "/api/leaderboard/rank/$playerA"
    $leaderboardType = Get-RedisValue $masterService @('TYPE', 'tournament:main')
    Assert-Demo ($scoreA.score -eq 120 -and $scoreB.score -eq 80 -and $scoreA2.score -eq 125 -and $top.Count -gt 0 -and $rank.rank -ge 0 -and $leaderboardType -eq 'zset') "Sorted Set: очки 120 + 5 = 125; топ-10 и место $($rank.rank) доступны"
    Write-Host "Топ-3: $(ConvertTo-Json -InputObject @($top | Select-Object -First 3) -Compress)"
    $added = Invoke-Api 'POST' "/api/players/$playerA/achievements" @{ name = 'first_win' }
    $duplicate = Invoke-Api 'POST' "/api/players/$playerA/achievements" @{ name = 'first_win' }
    $null = Invoke-Api 'POST' "/api/players/$playerB/achievements" @{ name = 'first_win' }
    $has = Invoke-Api 'GET' "/api/players/$playerA/achievements/first_win"
    $common = Invoke-Api 'GET' "/api/players/$playerA/achievements/common/$playerB"
    $achievementType = Get-RedisValue $masterService @('TYPE', "achievements:$playerA")
    Assert-Demo ($added.added -and -not $duplicate.added -and $has.hasAchievement -and $common -contains 'first_win' -and $achievementType -eq 'set') 'Set: добавление, дубль, проверка и пересечение'
    Write-Host "Общие достижения: $($common -join ', ')"

    Show-Step '4. Redis Stream, consumer и SSE fan-out'
    $jobs = @()
    try {
        $jobs = @(Start-SseReader $playerA; Start-SseReader $playerA)
        Wait-SseConnected $jobs
        $changed = Invoke-Api 'PATCH' "/api/players/$playerA/level" @{ delta = 3 }
        $null = Wait-Job -Job $jobs -Timeout 20
        $frames = @($jobs | ForEach-Object {
            @(Receive-Job $_ -Keep) | Where-Object { $_ -is [psobject] -and $_.PSObject.Properties['Payload'] } | Select-Object -Last 1
        })
        Assert-Demo ($jobs[0].State -eq 'Completed' -and $jobs[1].State -eq 'Completed' -and $frames.Count -eq 2) 'два SSE-клиента получили событие'
        Assert-Demo ($frames[0].Id -eq $frames[1].Id -and $frames[0].Kind -eq 'level_changed' -and $frames[0].Payload.playerId -eq $playerA -and $changed.level -eq 5) "оба клиента получили Redis ID $($frames[0].Id), уровень стал 5"
        Write-Host "SSE: event=$($frames[0].Kind) id=$($frames[0].Id) data=$(ConvertTo-Json -InputObject $frames[0].Payload -Compress)"
    } finally {
        foreach ($job in $jobs) {
            if ($job.State -eq 'Running') { Stop-Job $job }
            Remove-Job $job -Force
        }
    }
    $streamLength = [int](Get-RedisValue $masterService @('XLEN', 'notifications'))
    $pending = -1
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        $pending = [int](Get-RedisValue $masterService @('XPENDING', 'notifications', 'notifications-group'))
        if ($pending -eq 0) { break }
        Start-Sleep -Milliseconds 200
    }
    Assert-Demo ($streamLength -gt 0 -and $pending -eq 0) "Redis Stream содержит $streamLength сообщений; consumer сделал XACK"

    Show-Step '5. Ошибки API'
    $missing = Get-ApiStatus 'GET' "/api/players/demo-$runId-missing"
    $invalid = Get-ApiStatus 'POST' "/api/players/$playerA" @{ name = ''; level = -1; region = '' }
    Assert-Demo ($missing -eq 404 -and $invalid -eq 400) 'отсутствующий профиль = 404, неверные данные = 400'

    if ($WithFailover) {
        Show-Step '6. Sentinel failover (остановка мастера)'
        $oldMaster = $masterService
        $oldIp = $masterIp
        try {
            & docker compose stop $oldMaster | Out-Host
            if ($LASTEXITCODE -ne 0) { throw "Не удалось остановить $oldMaster" }
            $deadline = (Get-Date).AddSeconds($FailoverTimeoutSeconds)
            $started = Get-Date
            $newIp = $oldIp
            $newMaster = $null
            do {
                Start-Sleep -Seconds 2
                try {
                    $candidateIp = Get-MasterIp
                    if ($candidateIp -ne $oldIp) {
                        $candidateMaster = Get-MasterService $candidateIp
                        $candidateReplication = @(Invoke-Redis $candidateMaster @('INFO', 'replication'))
                        if ($candidateReplication -contains 'role:master') {
                            $newIp = $candidateIp
                            $newMaster = $candidateMaster
                            break
                        }
                    }
                } catch {
                    # Sentinel and Redis may briefly be unavailable during election.
                }
                $elapsed = [int]((Get-Date) - $started).TotalSeconds
                if ($elapsed % 10 -lt 2) {
                    Write-Host "Ожидаю failover: прошло $elapsed из $FailoverTimeoutSeconds с; Sentinel: $candidateIp"
                }
            } while ((Get-Date) -lt $deadline)
            Assert-Demo ($null -ne $newMaster) "Sentinel переключил мастер: $oldIp -> $newIp ($newMaster; лимит $FailoverTimeoutSeconds с)"
            $loginAfterFailover = $null
            for ($attempt = 0; $attempt -lt 10 -and -not $loginAfterFailover; $attempt++) {
                try { $loginAfterFailover = Invoke-Api 'POST' "/api/players/$playerA/login" }
                catch { Start-Sleep -Seconds 2 }
            }
            Assert-Demo ($null -ne $loginAfterFailover) "API продолжает запись через $newMaster"
        } finally {
            & docker compose start $oldMaster | Out-Host
            if ($LASTEXITCODE -ne 0) { throw "Не удалось восстановить $oldMaster" }
        }
        $masterService = Get-MasterService (Get-MasterIp)
        Assert-Demo (Wait-ForTwoReplicas $masterService $FailoverTimeoutSeconds) 'после восстановления снова подключены две реплики'
        Restore-ReplicaRuntimeConfig $oldMaster
    }

    if ($WithSplitBrain) {
        Show-Step '7. Защита от split-brain (пауза двух реплик)'
        $masterService = Get-MasterService (Get-MasterIp)
        $replicas = @($redisServices | Where-Object { $_ -ne $masterService })
        $running = @(& docker compose ps --status running --services)
        foreach ($replica in $replicas) {
            Assert-Demo ($running -contains $replica) "$replica запущена перед паузой"
        }
        $paused = @()
        try {
            foreach ($replica in $replicas) {
                $paused += $replica
                & docker compose pause $replica | Out-Host
                if ($LASTEXITCODE -ne 0) { throw "Не удалось поставить на паузу $replica" }
            }
            Assert-Demo (Wait-ForNoGoodReplicas $masterService) 'мастер больше не видит пригодных реплик'
            $probeKey = "demo:split-brain:$runId"
            $writeResult = @(& docker compose exec -T $masterService redis-cli --raw SET $probeKey 1 EX 30 2>&1)
            Assert-Demo (($writeResult -join ' ') -match 'NOREPLICAS') "мастер отклонил запись без живых реплик (получено: $($writeResult -join ' '))"
        } finally {
            $restoreFailures = @()
            foreach ($replica in $paused) {
                & docker compose unpause $replica | Out-Host
                if ($LASTEXITCODE -ne 0) { $restoreFailures += $replica }
            }
            if ($restoreFailures.Count -gt 0) { throw "Не удалось снять паузу с: $($restoreFailures -join ', ')" }
        }
        Assert-Demo (Wait-ForTwoReplicas $masterService) 'после снятия паузы снова подключены две реплики'
    }

    Write-Host "`nДемонстрация завершена. ID игроков: $playerA, $playerB. Данные остались в Redis." -ForegroundColor Green
} finally {
    Pop-Location
}
