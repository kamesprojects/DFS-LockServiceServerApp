# Script starting/stopping DFS Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 3001) or address where to connect and stop the service (e.g. 127.0.0.1:3001)
# 3. Address and port where to connect Extent Service (format is IP:port, e.g. 127.0.0.1:2001)
# 4. Address and port where to connect Lock Service (format is IP:port, e.g. 127.0.0.1:1001)
#
# Examples how to use the script:
#
# .\dfsservice.ps1 start 3001 127.0.0.1:2001 127.0.0.1:1001
# .\dfsservice.ps1 stop 127.0.0.1:3001

# ==========================================================
# Script starting/stopping DFS Service (for Windows PowerShell)
#
# Usage:
#   .\dfsservice.ps1 start <port> <extentHost:port> <lockHost:port>
#   .\dfsservice.ps1 stop  <host:port>
# ==========================================================

param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("start","stop")]
  [string]$command,

  [Parameter(Mandatory=$true)]
  [string]$arg1,

  [Parameter(Mandatory=$true)]
  [string]$extentAddress,

  [string]$lockAddress
)

$ErrorActionPreference = "Stop"
$SERVICE = "dfsservice"
$JAR = "..\dfs-projects\target\dfs-skurla-lab-1.0-SNAPSHOT.jar"
$LOGDIR = "logs"
New-Item -ItemType Directory -Force -Path $LOGDIR | Out-Null

function Get-PortFromArg([string]$value) {
  if ($value -match "^[\w\.\-]+:(\d+)$") { return $Matches[1] }
  if ($value -match "^\d+$") { return $value }
  throw "Neplatny parameter pre port/adresu: $value"
}

switch ($command) {
  "start" {
    if (-not $arg1 -or -not $extentAddress -or -not $lockAddress) {
      throw "Pouzitie: .\dfsservice.ps1 start <port> <extentAddress> <lockAddress>"
    }
    $port = Get-PortFromArg $arg1
    $pidFile = "$LOGDIR\$SERVICE-$port.pid"
    $out = "$LOGDIR\$SERVICE-$port.out.log"
    $err = "$LOGDIR\$SERVICE-$port.err.log"

    if (!(Test-Path $JAR)) { throw "Chyba $JAR. Spusti 'mvn -U clean package'." }

    if (Test-Path $pidFile) {
      try {
        $oldPid = Get-Content $pidFile | Select-Object -First 1
        if ($oldPid) {
            Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
        }
      } catch {}
        Remove-Item $pidFile -ErrorAction SilentlyContinue
    }

    Write-Host "[INFO] Spustam $SERVICE na porte $port (Extent=$extentAddress, Lock=$lockAddress)..."
    $p = Start-Process -NoNewWindow -FilePath "java" `
      -ArgumentList @("-cp", $JAR, "dfs.dfs.DfsServiceServer", $port, $extentAddress, $lockAddress) `
      -RedirectStandardOutput $out -RedirectStandardError $err -PassThru

    $p.Id | Out-File -FilePath $pidFile -Encoding ascii -Force
    Write-Host "[OK] $SERVICE bezi (PID $($p.Id)). Logy: $out | $err"
  }

  "stop" {
    if (-not $arg1) { throw "Pouzitie: .\dfsservice.ps1 stop <address|port>" }
    $port = Get-PortFromArg $arg1
    $pidFile = "$LOGDIR\$SERVICE-$port.pid"
    if (Test-Path $pidFile) {
      $procId = Get-Content $pidFile | Select-Object -First 1
      if ($procId) {
        Write-Host "[INFO] Zastavujem $SERVICE na porte $port (PID $procId)..."
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
      }
      Remove-Item $pidFile -ErrorAction SilentlyContinue
      Write-Host "[OK] $SERVICE zastaveny."
    } else {
        Write-Host "[WARN] PID subor ($pidFile) neexistuje. Kontrola procesu podla portu..."
        $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($conn) {
          $fallbackPid = $conn.OwningProcess
          Write-Host "[WARN] PID subor nenajdeny, ukoncenie procesu na porte $port (PID $fallbackPid)..."
          Stop-Process -Id $fallbackPid -Force -ErrorAction SilentlyContinue
          Write-Host "[OK] $SERVICE zastaveny (fallback)."
        } else {
          Write-Host "[WARN] PID subor ($pidFile) neexistuje a na porte $port nikto nepocuva."
        }
    }
  }
}