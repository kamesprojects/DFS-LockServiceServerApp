package dfs.dfs;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import io.grpc.stub.StreamObserver;

public class LockCacheServiceImpl extends LockCacheServiceGrpc.LockCacheServiceImplBase {

    private final dfs.dfs.LockClient client;

    public LockCacheServiceImpl(dfs.dfs.LockClient client) { this.client = client; }

    @Override
    public void revoke(LockCacheServiceOuterClass.RevokeRequest req,
                       StreamObserver<LockCacheServiceOuterClass.RevokeResponse> rsp) {
        client.onRevoke(req.getLockId());
        rsp.onNext(LockCacheServiceOuterClass.RevokeResponse.newBuilder().build());
        rsp.onCompleted();
    }

    @Override
    public void retry(LockCacheServiceOuterClass.RetryRequest req,
                      StreamObserver<LockCacheServiceOuterClass.RetryResponse> rsp) {
        client.onRetry(req.getLockId(), req.getSequence());
        rsp.onNext(LockCacheServiceOuterClass.RetryResponse.newBuilder().build());
        rsp.onCompleted();
    }
}

