package dfs.lock;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public final class LockServer implements Callable<Integer> {

    @CommandLine.Option(names="--port", required = true, description="Port to listen on")
    int port;

    private Server server;

    public static void main(String[] args) {
        System.exit(new CommandLine(new LockServer()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        server = ServerBuilder.forPort(port)
                .addService(new LockServiceImpl(this::shutdown))
                .build()
                .start();

        System.out.println("[LockService] listening on port " + port);
        try { server.awaitTermination(); } catch (InterruptedException ignored) {}
        return 0;
    }

    private void shutdown() {
        if (server == null) return;
        server.shutdown();
        try {
            if (!server.awaitTermination(2, TimeUnit.SECONDS)) server.shutdownNow();
        } catch (InterruptedException e) {
            server.shutdownNow();
        }
    }
}