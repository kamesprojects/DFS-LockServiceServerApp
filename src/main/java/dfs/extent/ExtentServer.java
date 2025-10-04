package dfs.extent;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import picocli.CommandLine;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public final class ExtentServer implements Callable<Integer> {

    @CommandLine.Option(names="--port", required = true, description="Port to listen on")
    int port;

    @CommandLine.Option(names="--root", required = true, description="Root directory for extent")
    String root;

    private Server server;

    public static void main(String[] args) {
        System.exit(new CommandLine(new ExtentServer()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        Runnable onStop = this::shutdown;

        server = ServerBuilder.forPort(port)
                .addService(new ExtentServiceImpl(Path.of(root), onStop))
                .build()
                .start();

        System.out.println("[ExtentService] listening on port " + port + " root=" + root);
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
