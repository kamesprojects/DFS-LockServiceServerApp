package dfs.lock;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import dfs.lock.LockServiceOuterClass.AcquireRequest;
import dfs.lock.LockServiceOuterClass.AcquireResponse;
import dfs.lock.LockServiceOuterClass.ReleaseRequest;
import dfs.lock.LockServiceOuterClass.ReleaseResponse;
import dfs.lock.LockServiceOuterClass.StopRequest;
import dfs.lock.LockServiceOuterClass.StopResponse;

public final class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {

    private static final class LockState {
        final ReentrantLock lock = new ReentrantLock();
        final Condition cond = lock.newCondition();
        boolean locked = false;
    }

    private final ConcurrentHashMap<String, LockState> locks = new ConcurrentHashMap<>();
    private LockState state(String id) { return locks.computeIfAbsent(id, k -> new LockState()); }

    private final Runnable onStop;
    public LockServiceImpl(Runnable onStop) { this.onStop = onStop; }

    @Override
    public void acquire(AcquireRequest req, StreamObserver<AcquireResponse> resp) {
        String id = req.getLockId();
        LockState st = state(id);

        st.lock.lock();
        try {
            while (st.locked) st.cond.awaitUninterruptibly();
            st.locked = true;
            resp.onNext(AcquireResponse.newBuilder().setSuccess(true).build());
            resp.onCompleted();
        } finally {
            st.lock.unlock();
        }
    }

    @Override
    public void release(ReleaseRequest req, StreamObserver<ReleaseResponse> resp) {
        String id = req.getLockId();
        LockState st = state(id);

        st.lock.lock();
        try {
            st.locked = false;
            st.cond.signalAll();
            resp.onNext(ReleaseResponse.newBuilder().build());
            resp.onCompleted();
        } finally {
            st.lock.unlock();
        }
    }

    @Override
    public void stop(StopRequest req, StreamObserver<StopResponse> resp) {
        resp.onNext(StopResponse.getDefaultInstance());
        resp.onCompleted();
        if (onStop != null) onStop.run();
    }
}
