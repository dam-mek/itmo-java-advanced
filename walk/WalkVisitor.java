package info.kgeorgiy.ja.denisov.walk;


import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;

public class WalkVisitor extends SimpleFileVisitor<Path> {
    private final BufferedWriter writer;
    private final static int BUFFER_SIZE = 1 << 12;
    private final byte[] buffer = new byte[BUFFER_SIZE];

    WalkVisitor(BufferedWriter writer) {
        this.writer = writer;
    }

    void writeInvalidPath(Path filepath, Exception e) throws IOException {
        writeInvalidPath(filepath.toString(), e);
    }

    void writeInvalidPath(String filepath, Exception e) throws IOException {
        write(filepath, 0);
        System.err.println(e.getMessage());
    }

    private void write(String filepath, int hash) throws IOException {
        writer.write(String.format("%08x %s%n", hash, filepath));
    }

    @Override
    public FileVisitResult visitFile(Path filepath, BasicFileAttributes attrs) throws IOException {
        try (InputStream inputStream = Files.newInputStream(filepath)) {
            int hash = 0;
            int size;
            while ((size = inputStream.read(buffer)) > 0) {
                for (int i = 0; i < size; i++) {
                    hash += buffer[i] & 0xff;
                    hash += hash << 10;
                    hash ^= hash >>> 6;
                }
            }
            hash += hash << 3;
            hash ^= hash >>> 11;
            hash += hash << 15;

            write(filepath.toString(), hash);
        } catch (IOException e) {
            writeInvalidPath(filepath, e);
            System.err.println(e.getMessage());
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path filepath, IOException e) throws IOException {
        writeInvalidPath(filepath, e);
        return FileVisitResult.CONTINUE;
    }
}