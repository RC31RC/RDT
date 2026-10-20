package io.github.rdt.console.connection;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.support.JsonCodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** Local administrative secrets: encrypted at rest, private files, durable atomic replacement. */
public class ConsoleConnectionStore {
    private static final byte[] MAGIC = { 'R', 'D', 'T', 1 };
    private final Path directory;
    private final JsonCodec json = new JsonCodec();
    private final SecureRandom random = new SecureRandom();

    public ConsoleConnectionStore(Path directory) { this.directory = directory; }

    public ConsoleConnections load() {
        Path file = directory.resolve("connections.enc");
        if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return ConsoleConnections.empty();
        try {
            rejectSymlink(file);
            if (Files.size(file) > 1024 * 1024) throw new IOException("Settings file exceeds 1 MiB");
            byte[] raw = Files.readAllBytes(file);
            if (raw.length < 32 || !Arrays.equals(Arrays.copyOfRange(raw, 0, 4), MAGIC)) {
                throw new IOException("Unsupported settings file format");
            }
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key(false), new GCMParameterSpec(128, raw, 4, 12));
            cipher.updateAAD(MAGIC);
            String plain = new String(cipher.doFinal(raw, 16, raw.length - 16), StandardCharsets.UTF_8);
            return json.read(plain, ConsoleConnections.class);
        } catch (IOException | GeneralSecurityException failure) {
            throw new RdtConfigurationException("Cannot read Console connections; check connections.enc and connections.key", failure);
        }
    }

    public void save(ConsoleConnections settings) {
        try {
            prepareDirectory();
            byte[] nonce = new byte[12];
            random.nextBytes(nonce);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key(true), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(MAGIC);
            byte[] encrypted = cipher.doFinal(json.write(settings).getBytes(StandardCharsets.UTF_8));
            ByteBuffer payload = ByteBuffer.allocate(16 + encrypted.length).put(MAGIC).put(nonce).put(encrypted);
            payload.flip();
            atomicWrite(directory.resolve("connections.enc"), payload);
        } catch (IOException | GeneralSecurityException failure) {
            throw new RdtConfigurationException("Cannot persist Console connections; check the private settings directory", failure);
        }
    }

    private SecretKeySpec key(boolean create) throws IOException {
        Path file = directory.resolve("connections.key");
        rejectSymlink(file);
        if (!Files.exists(file) && create) {
            byte[] raw = new byte[32];
            random.nextBytes(raw);
            // CREATE_NEW never replaces an existing key, including one created by another process.
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                privateFile(file);
                writeFully(channel, ByteBuffer.wrap(raw));
            }
        }
        byte[] raw = Files.readAllBytes(file);
        if (raw.length != 32) throw new IOException("Invalid local encryption key");
        return new SecretKeySpec(raw, "AES");
    }

    private void prepareDirectory() throws IOException {
        rejectSymlink(directory);
        Files.createDirectories(directory);
        if (Files.getFileStore(directory).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        }
    }

    private void atomicWrite(Path target, ByteBuffer bytes) throws IOException {
        rejectSymlink(target);
        Path temporary = Files.createTempFile(directory, ".connections-", ".tmp");
        try {
            privateFile(temporary);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                writeFully(channel, bytes);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private void privateFile(Path path) throws IOException {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) channel.write(bytes);
        channel.force(true);
    }

    private static void rejectSymlink(Path path) throws IOException {
        if (Files.isSymbolicLink(path)) throw new IOException("Settings paths must not be symbolic links");
    }
}
