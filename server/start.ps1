$ErrorActionPreference = 'Stop'
$javac = (Get-Command javac -ErrorAction Stop).Source
$java = Join-Path (Split-Path $javac) 'java.exe'
$build = Join-Path $PSScriptRoot 'build\main'
New-Item -ItemType Directory -Force -Path $build | Out-Null
$sources = Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'src\main\java') -Recurse -Filter '*.java' | ForEach-Object FullName
& $javac -encoding UTF-8 -d $build $sources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $java -cp $build lab.redis.RedisLabServer
