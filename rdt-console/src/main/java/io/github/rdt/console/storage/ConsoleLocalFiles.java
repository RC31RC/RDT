package io.github.rdt.console.storage;

import io.github.rdt.exception.RdtConfigurationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;

/** One private local directory; persistence failures never become an empty successful configuration. */
public final class ConsoleLocalFiles {
    private final Path directory;

    public ConsoleLocalFiles(Path directory) { this.directory = directory; }

    public byte[] read(String name) {
        Path path = directory.resolve(name);
        try {
            rejectSymlink(directory);
            rejectSymlink(path);
            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return null;
            if (Files.size(path) > 1024 * 1024) throw new IOException("Local settings exceed 1 MiB");
            return Files.readAllBytes(path);
        } catch (IOException failure) {
            throw new RdtConfigurationException("Cannot read Console local file: " + name, failure);
        }
    }

    public void write(String name, byte[] bytes) {
        Path path = directory.resolve(name);
        try {
            rejectSymlink(directory);
            rejectSymlink(path);
            Files.createDirectories(directory);
            boolean posix = Files.getFileStore(directory).supportsFileAttributeView("posix");
            if (posix) Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
            Path temporary = posix
                    ? Files.createTempFile(directory, ".rdt-", ".tmp", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
                    : Files.createTempFile(directory, ".rdt-", ".tmp");
            try {
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    while (buffer.hasRemaining()) channel.write(buffer);
                    channel.force(true);
                }
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } finally { Files.deleteIfExists(temporary); }
        } catch (IOException failure) {
            throw new RdtConfigurationException("Cannot persist Console local file: " + name, failure);
        }
    }

    private static void rejectSymlink(Path path) throws IOException {
        if (Files.isSymbolicLink(path)) throw new IOException("Console settings must not use symbolic links");
    }
}
