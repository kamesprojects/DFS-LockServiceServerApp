# Distributed File System - Lab 01

Tento projekt obsahuje implementáciu **LockService**, **ExtentService** a **DfsService** pomocou gRPC a Protobuf.

## 1. Build projektu

Najprv je potrebné vygenerovať triedy z `.proto` a skompilovať zdrojové kódy.

```powershell
# prečistenie a kompilácia (vygenerujú sa triedy z .proto)
mvn -U clean compile

# vytvorenie spustiteľných JAR súborov (fat JAR)
mvn -U clean package
```

Po úspešnom build-e sa v priečinku `target/` objavia JAR súbory:

- `lockservice-server.jar`
- `extentservice-server.jar`
- `dfsservice-server.jar`

## 2. Spustenie služieb

Každá služba má pripravený PowerShell skript v koreňovom adresári projektu.

### LockService

```powershell
# spustenie na porte 50051
.\lockservice.ps1 start 50051

# zastavenie
.\lockservice.ps1 stop 50051
```

### ExtentService

```powershell
# spustenie na porte 50052 s root priečinkom ./extent-root
.\extentservice.ps1 start 50052 .\extent-root

# zastavenie
.\extentservice.ps1 stop 50052
```

### DfsService

```powershell
# spustenie na porte 50053 s adresami Extent a Lock
.\dfsservice.ps1 start 50053 127.0.0.1:50052 127.0.0.1:50051

# zastavenie
.\dfsservice.ps1 stop 50053
```

## 3. Testovanie služieb

Na overenie, či služba beží na danom porte, je možné použiť príkazy:

### Test pomocou PowerShellu

```powershell
# overenie pripojenia na port LockService (50051)
Test-NetConnection 127.0.0.1 -Port 50051

# overenie pripojenia na port ExtentService (50052)
Test-NetConnection 127.0.0.1 -Port 50052

# overenie pripojenia na port DfsService (50053)
Test-NetConnection 127.0.0.1 -Port 50053
```

### Test pomocou netstat

```powershell
# zobraziť procesy, ktoré počúvajú na porte 50051
netstat -ano | findstr :50051

# zobraziť procesy, ktoré počúvajú na porte 50052
netstat -ano | findstr :50052

# zobraziť procesy, ktoré počúvajú na porte 50053
netstat -ano | findstr :50053
```

Ak port stále vykazuje ako obsadený aj po `stop`, je možné zistiť PID procesu:

```powershell
Get-NetTCPConnection -LocalPort 50051 -State Listen | Format-List
Get-Process -Id <PID>
Stop-Process -Id <PID> -Force
```

## 4. Logy

Každá služba ukladá logy do priečinka `logs/`:

- `lockservice-<port>.out.log` a `lockservice-<port>.err.log`
- `extentservice-<port>.out.log` a `extentservice-<port>.err.log`
- `dfsservice-<port>.out.log` a `dfsservice-<port>.err.log`

Tieto logy slúžia na debugovanie a sledovanie priebehu.

## 5. Poznámky

- Pre správne fungovanie je potrebné mať nainštalovanú Javu (minimálne Java 17).
- `.proto` súbory sú umiestnené v `src/main/proto` a počas buildu sa generujú Java triedy.
- Služby implementujú RPC `stop`, ktoré je možné volať aj programovo cez klienta DFS.
- Pre manuálne zastavenie bežiaceho procesu slúžia `.ps1` skripty.  
