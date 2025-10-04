package dfs.dfs;

import io.grpc.stub.StreamObserver;
import dfs.dfs.DfsServiceGrpc;
import dfs.dfs.DfsServiceOuterClass.*;

public class DfsServiceImpl extends DfsServiceGrpc.DfsServiceImplBase {
    private final String extentAddr;
    private final String lockAddr;
    private final Runnable onStop;

    public DfsServiceImpl(String extentAddr, String lockAddr, Runnable onStop) {
        this.extentAddr = extentAddr;
        this.lockAddr = lockAddr;
        this.onStop = onStop;
    }

    @Override public void stop(StopRequest req, StreamObserver<StopResponse> resp) {
        resp.onNext(StopResponse.getDefaultInstance());
        resp.onCompleted();
        if (onStop != null) onStop.run();
    }

    // TODO: dir/mkdir/rmdir/get/put/delete – volaj cez gRPC Extent/Lock služby.
}
