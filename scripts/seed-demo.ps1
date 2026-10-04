param([string]$BaseUrl = 'http://localhost:8080')

$ErrorActionPreference = 'Stop'
$players = 1001..1020 | ForEach-Object {
    @{ id = "$_"; name = "Player $_"; level = 1; region = 'eu' }
}
$body = ConvertTo-Json -InputObject @($players) -Depth 5 -Compress
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/players/batch" -ContentType 'application/json' -Body $body | Out-Null

Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/leaderboard/score" -ContentType 'application/json' -Body '{"playerId":"1001","score":120}' | Out-Null
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/leaderboard/score" -ContentType 'application/json' -Body '{"playerId":"1002","score":80}' | Out-Null
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/players/1001/achievements" -ContentType 'application/json' -Body '{"name":"first_win"}' | Out-Null
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/players/1002/achievements" -ContentType 'application/json' -Body '{"name":"first_win"}' | Out-Null
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/players/1001/login" | Out-Null
Invoke-RestMethod -Method Patch -Uri "$BaseUrl/api/players/1001/level" -ContentType 'application/json' -Body '{"delta":2}' | Out-Null
Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/players/1001" | Out-Null

Write-Host 'Demo data created: 20 profiles, scores, achievements, login counter, notification and cache.'
