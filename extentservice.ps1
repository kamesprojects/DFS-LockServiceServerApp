# ================================================
# Extent Service control script (Windows PowerShell)
# Compatible with dfs-grading-0.10 test scripts
# -----------------------------------------------
# Usage:
#   .\extentservice.ps1 start <port> <rootDir>
#   .\extentservice.ps1 stop <host:port>
#
# Example:
#   .\extentservice.ps1 start 2001 C:\extent-root
#   .\extentservice.ps1 stop 127.0.0.1:2001
# ================================================

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("start", "stop")]
    [string]$action,

    [Parameter(Mandatory = $true)]
    [string]$arg2,

    [string]$rootDir
)

$jarPath = "..\dfs-projects\target\dfs-skurla-lab-1.0-SNAPSHOT.jar"
$mainClass = "dfs.extent.ExtentServiceServer"

if ($action -eq "start") {
    if (-not $rootDir) {
        Write-Host "Usage: .\extentservice.ps1 start <port> <rootDir>"
        exit 1
    }

    $port = $arg2
    Write-Host "Starting Extent Service on port $port with root '$rootDir'..."

    if (-not (Test-Path $rootDir)) {
        New-Item -ItemType Directory -Force -Path $rootDir | Out-Null
    }

    Start-Process -FilePath "java" -ArgumentList "-cp `"$jarPath`" $mainClass $port $rootDir" `
        -WindowStyle Hidden

    Write-Host "Extent Service started."
}
elseif ($action -eq "stop") {
    $address = $arg2
    Write-Host "Stopping Extent Service at $address..."
    java -cp $jarPath dfs.client.StopClient dfs.extent.ExtentService $address 2>$null
    Write-Host "Stop request sent (if supported)."
}
else {
    Write-Host "Usage:"
    Write-Host "  .\extentservice.ps1 start <port> <rootDir>"
    Write-Host "  .\extentservice.ps1 stop <host:port>"
    exit 1
}