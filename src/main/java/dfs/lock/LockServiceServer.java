package dfs.lock;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class LockServiceServer {

    private final int port;
    private Server server;
    private LockServiceImpl impl;

    public LockServiceServer(int port) { this.port = port; }

    public void start() throws Exception {
        impl = new LockServiceImpl();
        server = ServerBuilder.forPort(port)
                .addService(impl)
                .build()
                .start();
        System.out.printf("Lock Service started on port %d%n", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (server != null) server.shutdown();
            if (impl != null) impl.stopBackground();
        }));
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) { System.exit(1); }
        var srv = new LockServiceServer(Integer.parseInt(args[0]));
        srv.start();
        srv.blockUntilShutdown();
    }

    public static class StopHandler extends LockServiceGrpc.LockServiceImplBase {
        private final Runnable onStop;
        public StopHandler(Runnable onStop) { this.onStop = onStop; }
        @Override public void stop(dfs.lock.LockServiceOuterClass.StopRequest req,
                                   io.grpc.stub.StreamObserver<dfs.lock.LockServiceOuterClass.StopResponse> resp) {
            resp.onNext(dfs.lock.LockServiceOuterClass.StopResponse.newBuilder().build());
            resp.onCompleted();
            if (onStop != null) new Thread(onStop).start();
        }
    }
}
