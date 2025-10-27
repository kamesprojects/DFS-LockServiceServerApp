package dfs.dfs;

import dfs.extent.ExtentServiceGrpc;
import dfs.lock.LockServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class DfsServiceServer {

    private final int port;
    private final String extentAddr;
    private final String lockAddr;
    private Server server;
    private ManagedChannel extentCh, lockCh;

    public DfsServiceServer(int port, String extentAddr, String lockAddr) {
        this.port = port;
        this.extentAddr = extentAddr;
        this.lockAddr = lockAddr;
    }

    public void start() throws Exception {
        extentCh = buildChannel(extentAddr);
        lockCh   = buildChannel(lockAddr);

        var extentStub = ExtentServiceGrpc.newBlockingStub(extentCh);
        var lockStub = LockServiceGrpc.newBlockingStub(lockCh);

        String ownerId = OwnerIdUtil.buildOwnerId(port);
        var lockClient = new LockClient(lockStub, ownerId);

        Runnable shutdownCb = () -> {
            System.out.println("Stopping DFS Server.");
            if (server != null) server.shutdown();
            lockClient.stop();
        };

        var impl = new DfsServiceImpl(extentStub, lockClient, shutdownCb);

        server = ServerBuilder.forPort(port)
                .addService(impl)
                .addService(new LockCacheServiceImpl(lockClient))
                .build()
                .start();

        System.out.printf("DFS Server started on port %d (Extent %s, Lock %s, OwnerId %s)%n",
                port, extentAddr, lockAddr, ownerId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (server != null) server.shutdown();
            if (extentCh != null) extentCh.shutdownNow();
            if (lockCh != null) lockCh.shutdownNow();
            lockClient.stop();
        }));
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }

    private static ManagedChannel buildChannel(String hostPort) {
        String[] hp = hostPort.split(":", 2);
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        return io.grpc.ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java dfs.dfs.DfsServiceServer <port> <extentHost:port> <lockHost:port>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        String extent = args[1];
        String lock = args[2];

        DfsServiceServer srv = new DfsServiceServer(port, extent, lock);
        srv.start();
        srv.blockUntilShutdown();
    }
}