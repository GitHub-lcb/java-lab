$ErrorActionPreference = 'Stop'
$javac = (Get-Command javac -ErrorAction Stop).Source
$java = Join-Path (Split-Path $javac) 'java.exe'
$build = Join-Path $PSScriptRoot 'build\test'
New-Item -ItemType Directory -Force -Path $build | Out-Null
$sources = @(
  (Join-Path $PSScriptRoot 'src\main\java\lab\redis\CommandParser.java'),
  (Join-Path $PSScriptRoot 'src\main\java\lab\redis\RedisCommandPolicy.java'),
  (Join-Path $PSScriptRoot 'src\test\java\lab\redis\CommandPolicyTest.java')
)
& $javac -encoding UTF-8 -d $build $sources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $java -cp $build lab.redis.CommandPolicyTest
exit $LASTEXITCODE
