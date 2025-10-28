# Script starting/stopping Lock Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 1001) or address where to connect and stop the service (e.g. 127.0.0.1:1001)
#
# Examples how to use the script:
#
# .\lockservice.ps1 start 1001
# .\lockservice.ps1 stop 127.0.0.1:1001

param(
  [Parameter(Mandatory=$true)]
  [ValidateSet('start','stop')]
  [string]$action,

  [Parameter(Mandatory=$true)]
  [string]$arg
)

$ErrorActionPreference = 'Stop'

$jar = Join-Path $PSScriptRoot '../dfs-projects/target/dfs-skurla-lab-1.0-SNAPSHOT.jar'
$SERVICE = "lockservice"
$LOGDIR = Join-Path $PSScriptRoot 'logs'

if (-not (Test-Path $LOGDIR)) { New-Item -ItemType Directory -Path $LOGDIR | Out-Null }

function Ensure-Jar {
  if (-not (Test-Path $jar)) {
    Write-Host 'Building shaded JAR...' -ForegroundColor Yellow
    mvn -q -DskipTests package
  }
}

function Get-PortFromArg([string]$value) {
  if ($value -match "^[\w\.\-]+:(\d+)$") { return $Matches[1] }
  if ($value -match "^\d+$") { return $value }
  throw "Neplatny parameter pre port/adresu: $value"
}

switch ($action) {
  'start' {
    
    $port = Get-PortFromArg $arg
    $pidFile = "$LOGDIR\$SERVICE-$port.pid"
    $out = "$LOGDIR\$SERVICE-$port.out.log"
    $err = "$LOGDIR\$SERVICE-$port.err.log"
    Ensure-Jar

    if (Test-Path $pidFile) {
      try {
        $oldPid = Get-Content $pidFile | Select-Object -First 1
        if ($oldPid) { Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue }
      } catch {}
      Remove-Item $pidFile -ErrorAction SilentlyContinue
    }
    
    Write-Host "Starting Lock Service on port $port..." -ForegroundColor Cyan
  
    $p = Start-Process -NoNewWindow -FilePath 'java' `
    -ArgumentList @('-jar', $jar, $port) `
    -RedirectStandardOutput $out -RedirectStandardError $err -PassThru 

    $p.Id | Out-File -FilePath $pidFile -Encoding ascii -Force
    Write-Host "[OK] $SERVICE bezi (PID $($p.Id)). Logy: $out | $err"
  }

  'stop' {
    
    $port = Get-PortFromArg $arg
    $pidFile = "$LOGDIR\$SERVICE-$port.pid"

    if (Test-Path $pidFile) {
      $procId = Get-Content $pidFile | Select-Object -First 1
      if ($procId) {
        Write-Host "[INFO] Zastavujem $SERVICE na porte $port (PID $procId)..."
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
      }
      Remove-Item $pidFile -ErrorAction SilentlyContinue
      Write-Host "[OK] $SERVICE zastaveny." -ForegroundColor Cyan
    } else {
      Write-Host "[WARN] PID subor ($pidFile) neexistuje. Kontrola procesu podla portu..."
    }
  }
}
