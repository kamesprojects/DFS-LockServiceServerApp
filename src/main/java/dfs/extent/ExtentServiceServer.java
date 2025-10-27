package dfs.extent;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExtentServiceServer {
    private Server server;
    private final int port;
    private final Path extentRoot;

    public ExtentServiceServer(int port, String rootStr) {
        this.port = port;
        this.extentRoot = Paths.get(rootStr).toAbsolutePath().normalize();
        try { Files.createDirectories(this.extentRoot); }
        catch (IOException e) { System.err.println("FATAL: " + e.getMessage()); System.exit(1); }
    }

    public void start() throws IOException {
        ExtentServiceImpl service = new ExtentServiceImpl(extentRoot, this::stop);
        server = ServerBuilder.forPort(port).addService(service).build().start();
        System.out.println("Extent Service started on " + port + " root=" + extentRoot);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { System.err.println("JVM shutdown"); stop(); }));
    }

    public void stop() {
        if (server != null) {
            System.out.println("Stopping Extent Service...");
            server.shutdown();
            try {
                if (!server.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) server.shutdownNow();
            } catch (InterruptedException e) { server.shutdownNow(); Thread.currentThread().interrupt(); }
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && "stop".equalsIgnoreCase(args[0]) && args[1].contains(":")) {
            var hp = args[1].split(":",2);
            var ch = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                    .forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
            try { ExtentServiceGrpc.newBlockingStub(ch).stop(ExtentServiceOuterClass.StopRequest.getDefaultInstance());
                System.out.println("Stop RPC sent to " + args[1]); }
            finally { ch.shutdownNow(); }
            return;
        }
        if (args.length != 2) {
            System.err.println("Usage:\n  Start: java -jar <jar> <port> <extentRoot>\n   Stop: java -jar <jar> stop <host:port>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        ExtentServiceServer srv = new ExtentServiceServer(port, args[1]);
        srv.start();
        srv.blockUntilShutdown();
    }
}
