package dfs.dfs;

import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LockClient {
    public enum State { None, Free, Locked, Acquiring, Releasing }

    static class Entry {
        final ReentrantLock mu = new ReentrantLock();
        final Condition cv = mu.newCondition();
        State state = State.None;
        int waiters = 0;
        boolean revoked = false;
        long lastSeq = -1;
    }

    private final Map<String, Entry> table = new ConcurrentHashMap<>();
    private final LockServiceGrpc.LockServiceBlockingStub lockStub;
    private final String ownerId;
    private final AtomicLong nextSeq = new AtomicLong(1);
    private final BlockingQueue<String> toRelease = new LinkedBlockingQueue<>();
    private final Thread releaserThread;
    private volatile boolean running = true;

    public LockClient(LockServiceGrpc.LockServiceBlockingStub lockStub, String ownerId) {
        this.lockStub = lockStub;
        this.ownerId = ownerId;
        this.releaserThread = new Thread(this::releaserLoop, "lock-releaser");
        this.releaserThread.setDaemon(true);
        this.releaserThread.start();
    }

    private Entry e(String id) { return table.computeIfAbsent(id, k -> new Entry()); }

    public void acquire(String id) {
        Entry e = e(id);
        e.mu.lock();
        try {
            while (true) {
                if (e.state == State.Locked || e.state == State.Free) {
                    e.state = State.Locked;
                    return;
                }

                if (e.state == State.None || e.state == State.Acquiring) {
                    long seq = nextSeq.getAndIncrement();
                    e.lastSeq = seq;
                    e.state = State.Acquiring;

                    e.mu.unlock();
                    boolean ok = rpcAcquire(id, seq);
                    e.mu.lock();

                    if (ok) {
                        e.state = State.Locked;
                        return;
                    }
                }

                try { e.cv.await(); }
                catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            }
        } finally {
            e.mu.unlock();
        }
    }

    public void release(String id) {
        Entry e = e(id);
        e.mu.lock();
        try {
            if (e.state == State.Locked) {
                if (e.revoked) {
                    e.state = State.Releasing;
                    toRelease.offer(id);
                } else {
                    e.state = State.Free;
                }
                e.cv.signalAll();
            }
        } finally {
            e.mu.unlock();
        }
    }

    public void onRevoke(String id) {
        Entry e = e(id);
        e.mu.lock();
        try {
            e.revoked = true;

            if (e.state == State.Free) {
                e.state = State.Releasing;
                toRelease.offer(id);
            }

            e.cv.signalAll();
        } finally {
            e.mu.unlock();
        }
    }

    public void onRetry(String id, long seq) {
        Entry e = e(id);
        e.mu.lock();
        try {
            e.cv.signalAll();
        } finally {
            e.mu.unlock();
        }
    }

    public void stop() {
        running = false;
        releaserThread.interrupt();
    }

    private boolean rpcAcquire(String id, long seq) {
        try {
            var resp = lockStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder()
                    .setLockId(id).setOwnerId(ownerId).setSequence(seq).build());
            return resp.getSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    private void rpcRelease(String id, long seq) {
        try {
            lockStub.release(LockServiceOuterClass.ReleaseRequest.newBuilder()
                    .setLockId(id).setOwnerId(ownerId).build());
        } catch (Exception e) {
        }
    }

    private void releaserLoop() {
        while (running) {
            try {
                String id = toRelease.take();
                Entry e = e(id);

                long seq;
                e.mu.lock();
                try {
                    seq = e.lastSeq;
                } finally {
                    e.mu.unlock();
                }

                rpcRelease(id, seq);

                e.mu.lock();
                try {
                    e.revoked = false;
                    e.state = State.None;
                    e.cv.signalAll();
                } finally {
                    e.mu.unlock();
                }

            } catch (InterruptedException ie) {
                break;
            }
        }
    }
}