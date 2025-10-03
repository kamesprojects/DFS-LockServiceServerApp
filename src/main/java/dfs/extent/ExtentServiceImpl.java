package dfs.extent;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Collectors;
import com.google.protobuf.ByteString;

import dfs.extent.ExtentServiceOuterClass.GetRequest;
import dfs.extent.ExtentServiceOuterClass.GetResponse;
import dfs.extent.ExtentServiceOuterClass.PutRequest;
import dfs.extent.ExtentServiceOuterClass.PutResponse;
import dfs.extent.ExtentServiceOuterClass.StopRequest;
import dfs.extent.ExtentServiceOuterClass.StopResponse;

public class ExtenServiceImpl extends ExtentServiceGrpc.ExtentServiceImplBase {
    private final Path root;
    private final Runnable onStop;

    public ExtentServiceImpl(Path root, Runnable onStop) {
        this.root = root.toAbsolutePath().normalize();
        this.onStop = onStop;
    }

    private static void ensureDirName(String name) {
        if (!name.endsWith("/")) throw new IllegalArgumentException("Directory must end with /");
    }

    private static void ensureFileName(String name) {
        if (name.endsWith("/")) throw new IllegalArgumentException("File name must not end with /");
    }

    private Path mapDfsToFs(String dfsName) {
        if (!dfsName.startsWith("/")) throw new IllegalArgumentException("DFS path must be absolute");
        Path p = root.resolve(dfsName.substring(1)).normalize();
        if (!p.startsWith(root)) throw new IllegalArgumentException("Path escape outside root");
        return p;
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> resp) {
        String name = req.getFileName();
        GetResponse.Builder out = GetResponse.newBuilder();

        try {
            if (name.endsWith("/")) {
                ensureDirName(name);
                Path dir = mapDfsToFs(name);
                if (Files.isDirectory(dir)) {
                    String listing = Files.list(dir)
                            .sorted()
                            .map(p -> p.getFileName().toString() + (Files.isDirectory(p) ? "/" : ""))
                            .collect(Collectors.joining("\n"));
                    out.setFileData(ByteString.copyFrom(listing.getBytes()));
                }
            } else {
                ensureFileName(name);
                Path file = mapDfsToFs(name);
                if (Files.isRegularFile(file)) {
                    out.setFileData(ByteString.copyFrom(Files.readAllBytes(file)));
                }
            }
        } catch (Exception ignored) {
            // pri chybe nechaj fileData nevyplnené → "null"
        }

        resp.onNext(out.build());
        resp.onCompleted();
    }

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> resp) {
        String name = req.getFileName();
        boolean ok = false;

        try {
            if (name.endsWith("/")) {
                ensureDirName(name);
                Path dir = mapDfsToFs(name);
                if (req.hasFileData()) {            // create directory
                    Files.createDirectories(dir);
                    ok = true;
                } else {                            // delete empty dir
                    if (Files.isDirectory(dir)) {
                        try (var s = Files.list(dir)) {
                            if (s.findAny().isEmpty()) {
                                Files.delete(dir);
                                ok = true;
                            }
                        }
                    }
                }
            } else {
                ensureFileName(name);
                Path file = mapDfsToFs(name);
                if (req.hasFileData()) {            // create or overwrite file
                    Files.createDirectories(file.getParent());
                    Files.write(file, req.getFileData().toByteArray(),
                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    ok = true;
                } else {                            // delete file
                    if (Files.isRegularFile(file)) {
                        Files.delete(file);
                        ok = true;
                    }
                }
            }
        } catch (IOException ignored) {
            ok = false;
        }

        resp.onNext(PutResponse.newBuilder().setSuccess(ok).build());
        resp.onCompleted();
    }

    @Override
    public void stop(StopRequest req, StreamObserver<StopResponse> resp) {
        resp.onNext(StopResponse.getDefaultInstance());
        resp.onCompleted();
        if (onStop != null) onStop.run();
    }
}
