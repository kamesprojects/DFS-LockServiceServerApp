package dfs.extent;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import dfs.extent.ExtentServiceOuterClass.GetRequest;
import dfs.extent.ExtentServiceOuterClass.GetResponse;
import dfs.extent.ExtentServiceOuterClass.PutRequest;
import dfs.extent.ExtentServiceOuterClass.PutResponse;
import dfs.extent.ExtentServiceOuterClass.StopRequest;
import dfs.extent.ExtentServiceOuterClass.StopResponse;

public class ExtentServiceImpl extends ExtentServiceGrpc.ExtentServiceImplBase {

    private final Path rootPath;
    private final Runnable shutdownCallback;

    public ExtentServiceImpl(Path rootPath, Runnable shutdownCallback) {
        this.rootPath = rootPath.normalize().toAbsolutePath();
        this.shutdownCallback = shutdownCallback;
    }
    // ---- Helpers ----
    private static boolean isDirectoryName(String dfsName) {
        return dfsName != null && dfsName.endsWith("/");
    }

    private Path mapToRealPath(String dfsName) {
        String cleaned = dfsName;
        if (cleaned.startsWith("/")) cleaned = cleaned.substring(1);
        if (cleaned.endsWith("/")) cleaned = cleaned.substring(0, cleaned.length() - 1);
        Path p = rootPath.resolve(cleaned).normalize();
        if (!p.startsWith(rootPath)) {
            return rootPath.resolve("_denied_").resolve("x");
        }
        return p;
    }

    private byte[] dirListingBytes(Path dir) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return null;
        }
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path child : ds) {
                String name = child.getFileName().toString();
                if (Files.isDirectory(child)) {
                    names.add(name + "/");
                } else {
                    names.add(name);
                }
            }
        }

        String joined = String.join("\n", names);
        return joined.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void stop(StopRequest request, StreamObserver<StopResponse> responseObserver) {
        responseObserver.onNext(StopResponse.newBuilder().build());
        responseObserver.onCompleted();
        if (shutdownCallback != null) {
            new Thread(shutdownCallback).start();
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String dfsName = request.getFileName();
        if (dfsName == null || dfsName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("fileName is required").asRuntimeException());
            return;
        }

        Path real = mapToRealPath(dfsName);
        try {
            GetResponse.Builder b = GetResponse.newBuilder();

            if (isDirectoryName(dfsName)) {
                byte[] listing = dirListingBytes(real);
                if (listing != null) {
                    b.setFileData(com.google.protobuf.ByteString.copyFrom(listing));
                }

                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } else {
                if (Files.exists(real) && Files.isRegularFile(real)) {
                    byte[] data = Files.readAllBytes(real);
                    b.setFileData(com.google.protobuf.ByteString.copyFrom(data));
                }

                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        } catch (Exception e) {
            responseObserver.onNext(GetResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String dfsName = request.getFileName();
        boolean hasData = request.hasFileData();

        if (dfsName == null || dfsName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("fileName is required").asRuntimeException());
            return;
        }

        Path real = mapToRealPath(dfsName);
        boolean success = false;

        try {
            if (isDirectoryName(dfsName)) {

                if (hasData) {

                    Files.createDirectories(real);
                    success = Files.isDirectory(real);
                } else {

                    if (Files.exists(real) && Files.isDirectory(real)) {
                        try (DirectoryStream<Path> ds = Files.newDirectoryStream(real)) {
                            if (!ds.iterator().hasNext()) {
                                Files.delete(real);
                                success = !Files.exists(real);
                            } else {
                                success = false;
                            }
                        }
                    } else {
                        success = false;
                    }
                }
            } else {
                if (hasData) {
                    if (real.getParent() != null) Files.createDirectories(real.getParent());
                    byte[] data = request.getFileData().toByteArray();
                    Files.write(real, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    success = Files.exists(real) && Files.isRegularFile(real);
                } else {
                    if (Files.exists(real) && Files.isRegularFile(real)) {
                        Files.delete(real);
                        success = !Files.exists(real);
                    } else {
                        success = false;
                    }
                }
            }
        } catch (IOException e) {
            success = false;
        }

        responseObserver.onNext(PutResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
    }
}
