package dfs.lock;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;



public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {

    static class Waiter {
        final String ownerId;
        Waiter(String o){ this.ownerId = o; }
    }

    static class Row {
        String holderOwner;
        long holderSeq = -1;
        final Deque<Waiter> q = new ArrayDeque<>();
        boolean revokeSent = false;
        final Map<String, Long> lastSeqByOwner = new HashMap<>();
        final ReentrantLock mu = new ReentrantLock();
    }

    private final ConcurrentMap<String, Row> table = new ConcurrentHashMap<>();
    private final BlockingQueue<String> revokerQ = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> retrierQ = new LinkedBlockingQueue<>();
    private final Thread revokerThread, retrierThread;
    private volatile boolean running = true;

    public LockServiceImpl() {
        revokerThread = new Thread(this::revokerLoop, "revoker");
        retrierThread = new Thread(this::retrierLoop, "retrier");
        revokerThread.setDaemon(true);
        retrierThread.setDaemon(true);
        revokerThread.start();
        retrierThread.start();
    }

    private Row r(String id) { return table.computeIfAbsent(id, k -> new Row()); }

    @Override
    public void acquire(LockServiceOuterClass.AcquireRequest req,
                        StreamObserver<LockServiceOuterClass.AcquireResponse> respObs) {
        String id = req.getLockId();
        String owner = req.getOwnerId();
        long seq = req.getSequence();

        Row row = r(id);
        row.mu.lock();
        try {
            row.lastSeqByOwner.put(owner, Math.max(seq, row.lastSeqByOwner.getOrDefault(owner, -1L)));
            if (row.holderOwner == null) {
                row.holderOwner = owner;
                row.holderSeq = seq;
                row.revokeSent = false;
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder().setSuccess(true).build());
                respObs.onCompleted();
            } else if (row.holderOwner.equals(owner)) {
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder().setSuccess(true).build());
                respObs.onCompleted();
            } else {
                row.q.addLast(new Waiter(owner));
                if (!row.revokeSent) {
                    row.revokeSent = true;
                    revokerQ.offer(id);
                }
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder().setSuccess(false).build());
                respObs.onCompleted();
            }
        } finally {
            row.mu.unlock();
        }
    }

    @Override
    public void release(LockServiceOuterClass.ReleaseRequest req,
                        StreamObserver<LockServiceOuterClass.ReleaseResponse> respObs) {
        String id = req.getLockId();
        String owner = req.getOwnerId();

        Row row = r(id);
        row.mu.lock();
        try {
            if (owner.equals(row.holderOwner)) {
                row.holderOwner = null;
                row.holderSeq = -1;
                if (!row.q.isEmpty()) {
                    retrierQ.offer(id);
                }
            }
            respObs.onNext(LockServiceOuterClass.ReleaseResponse.newBuilder().build());
            respObs.onCompleted();
        } finally {
            row.mu.unlock();
        }
    }

    private void revokerLoop() {
        while (running) {
            try {
                String id = revokerQ.take();
                Row row = r(id);
                String holder;
                row.mu.lock();
                try { holder = row.holderOwner; } finally { row.mu.unlock(); }
                if (holder != null) {
                    callClient(holder, stub -> {
                        stub.revoke(LockCacheServiceOuterClass.RevokeRequest.newBuilder()
                                .setLockId(id).build());
                    });
                }
            } catch (InterruptedException ie) {
            } catch (Exception e) {
            }
        }
    }


    private void retrierLoop() {
        while (running) {
            try {
                String id = retrierQ.take();
                Row row = r(id);
                java.util.List<String> toNotify = new java.util.ArrayList<>();
                row.mu.lock();
                try {
                    while (!row.q.isEmpty() && row.holderOwner == null) {
                        toNotify.add(row.q.removeFirst().ownerId);
                    }
                    row.revokeSent = false;
                } finally {
                    row.mu.unlock();
                }
                for (String owner : toNotify) {
                    long seq = row.lastSeqByOwner.getOrDefault(owner, 0L);
                    callClient(owner, stub -> {
                        stub.retry(LockCacheServiceOuterClass.RetryRequest.newBuilder()
                                .setLockId(id).setSequence(seq).build());
                    });
                }
            } catch (InterruptedException ie) {
            } catch (Exception e) {
            }
        }
    }

    private void callClient(String ownerId, java.util.function.Consumer<LockCacheServiceGrpc.LockCacheServiceBlockingStub> call) {
        String[] parts = ownerId.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        try {
            var stub = LockCacheServiceGrpc.newBlockingStub(ch);
            call.accept(stub);
        } finally {
            ch.shutdownNow();
        }
    }

    public void stopBackground() {
        running = false;
        revokerThread.interrupt();
        retrierThread.interrupt();
    }
}
