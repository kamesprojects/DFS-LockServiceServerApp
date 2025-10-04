package dfs.dfs;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import picocli.CommandLine;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public final class DfsServer implements Callable<Integer> {

    @CommandLine.Option(names="--port", required=true, description="Port to listen on")
    int port;

    @CommandLine.Option(names="--extent", required=true, description="Extent address (host:port)")
    String extentAddress;

    @CommandLine.Option(names="--lock", required=true, description="Lock address (host:port)")
    String lockAddress;

    private Server server;

    public static void main(String[] args) {
        System.exit(new CommandLine(new DfsServer()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        Runnable onStop = this::shutdown;

        // TODO: vytvor si svoju implementáciu DfsServiceImpl (volá Extent a Lock)
        server = ServerBuilder.forPort(port)
                .addService(new DfsServiceImpl(extentAddress, lockAddress, onStop))
                .build()
                .start();

        System.out.println("[DfsService] listening on port " + port +
                " extent=" + extentAddress + " lock=" + lockAddress);
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
