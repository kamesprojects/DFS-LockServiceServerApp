package dfs.dfs;

import com.google.protobuf.ByteString;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class DfsServiceImpl extends DfsServiceGrpc.DfsServiceImplBase {

    private final ExtentServiceGrpc.ExtentServiceBlockingStub extent;
    private final LockClient lockClient;
    private final Runnable shutdownCb;

    public DfsServiceImpl(ExtentServiceGrpc.ExtentServiceBlockingStub extent,
                          LockClient lockClient,
                          Runnable shutdownCb) {
        this.extent = extent;
        this.lockClient = lockClient;
        this.shutdownCb = shutdownCb;
    }

    private static boolean isDir(String name) { return name != null && name.endsWith("/"); }

    private void acquire(String id) { lockClient.acquire(id); }
    private void release(String id) { lockClient.release(id); }

    private List<String> parseDirListing(byte[] data) {
        String text = new String(data, StandardCharsets.UTF_8);
        if (text.isEmpty()) return List.of();
        return Arrays.asList(text.split("\n"));
    }

    @Override
    public void stop(dfs.dfs.DfsServiceOuterClass.StopRequest request,
                     StreamObserver<dfs.dfs.DfsServiceOuterClass.StopResponse> responseObserver) {
        responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.StopResponse.newBuilder().build());
        responseObserver.onCompleted();
        if (shutdownCb != null) new Thread(shutdownCb).start();
    }

    @Override
    public void dir(dfs.dfs.DfsServiceOuterClass.DirRequest request,
                    StreamObserver<dfs.dfs.DfsServiceOuterClass.DirResponse> responseObserver) {

        String dirName = request.getDirectoryName();
        if (!isDir(dirName)) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.DirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        acquire(dirName);
        try {
            var resp = extent.get(ExtentServiceOuterClass.GetRequest.newBuilder()
                    .setFileName(dirName).build());

            if (!resp.hasFileData()) {
                responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.DirResponse.newBuilder()
                        .setSuccess(false).build());
            } else {
                var list = parseDirListing(resp.getFileData().toByteArray());
                var b = dfs.dfs.DfsServiceOuterClass.DirResponse.newBuilder()
                        .setSuccess(true).addAllDirList(list);
                responseObserver.onNext(b.build());
            }
            responseObserver.onCompleted();
        } finally {
            release(dirName);
        }
    }

    @Override
    public void mkdir(dfs.dfs.DfsServiceOuterClass.MkdirRequest request,
                      StreamObserver<dfs.dfs.DfsServiceOuterClass.MkdirResponse> responseObserver) {
        String dirName = request.getDirectoryName();
        if (!isDir(dirName)) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.MkdirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        acquire(dirName);
        try {
            var put = ExtentServiceOuterClass.PutRequest.newBuilder()
                    .setFileName(dirName)
                    .setFileData(ByteString.copyFrom(new byte[]{1}))
                    .build();
            boolean ok = extent.put(put).getSuccess();
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.MkdirResponse.newBuilder()
                    .setSuccess(ok).build());
            responseObserver.onCompleted();
        } finally {
            release(dirName);
        }
    }

    @Override
    public void rmdir(dfs.dfs.DfsServiceOuterClass.RmdirRequest request,
                      StreamObserver<dfs.dfs.DfsServiceOuterClass.RmdirResponse> responseObserver) {
        String dirName = request.getDirectoryName();
        if (!isDir(dirName)) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.RmdirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        acquire(dirName);
        try {
            var put = ExtentServiceOuterClass.PutRequest.newBuilder()
                    .setFileName(dirName)
                    .build();
            boolean ok = extent.put(put).getSuccess();
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.RmdirResponse.newBuilder()
                    .setSuccess(ok).build());
            responseObserver.onCompleted();
        } finally {
            release(dirName);
        }
    }

    @Override
    public void get(dfs.dfs.DfsServiceOuterClass.GetRequest request,
                    StreamObserver<dfs.dfs.DfsServiceOuterClass.GetResponse> responseObserver) {
        String fileName = request.getFileName();
        if (isDir(fileName)) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.GetResponse.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        acquire(fileName);
        try {
            var resp = extent.get(ExtentServiceOuterClass.GetRequest.newBuilder()
                    .setFileName(fileName).build());
            var b = dfs.dfs.DfsServiceOuterClass.GetResponse.newBuilder();
            if (resp.hasFileData()) b.setFileData(resp.getFileData());
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } finally {
            release(fileName);
        }
    }

    @Override
    public void put(dfs.dfs.DfsServiceOuterClass.PutRequest request,
                    StreamObserver<dfs.dfs.DfsServiceOuterClass.PutResponse> responseObserver) {
        String fileName = request.getFileName();
        if (isDir(fileName) || !request.hasFileData()) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.PutResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        acquire(fileName);
        try {
            var put = ExtentServiceOuterClass.PutRequest.newBuilder()
                    .setFileName(fileName)
                    .setFileData(request.getFileData())
                    .build();
            boolean ok = extent.put(put).getSuccess();
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.PutResponse.newBuilder()
                    .setSuccess(ok).build());
            responseObserver.onCompleted();
        } finally {
            release(fileName);
        }
    }

    @Override
    public void delete(dfs.dfs.DfsServiceOuterClass.DeleteRequest request,
                       StreamObserver<dfs.dfs.DfsServiceOuterClass.DeleteResponse> responseObserver) {
        String fileName = request.getFileName();
        if (isDir(fileName)) {
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.DeleteResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        acquire(fileName);
        try {
            var put = ExtentServiceOuterClass.PutRequest.newBuilder()
                    .setFileName(fileName)
                    .build();
            boolean ok = extent.put(put).getSuccess();
            responseObserver.onNext(dfs.dfs.DfsServiceOuterClass.DeleteResponse.newBuilder()
                    .setSuccess(ok).build());
            responseObserver.onCompleted();
        } finally {
            release(fileName);
        }
    }
}