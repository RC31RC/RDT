package io.github.rdt.console.archive;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streaming gzip with a same-directory staging file and an explicit abort boundary. */
public final class LocalFileArchiveSink implements RdtArchiveSink {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileArchiveSink.class);
    private static final Pattern ARCHIVE_NAME = Pattern.compile("rdt-archive-(\\d{4}-\\d{2}-\\d{2})\\.jsonl\\.gz");
    private final Path target;
    private final Path temporary;
    private final BufferedWriter writer;
    private final LocalDate date;
    private final int retentionDays;
    private boolean closed;

    public LocalFileArchiveSink(Path directory, LocalDate date, int retentionDays) {
        this.date = date;
        this.retentionDays = retentionDays;
        this.target = directory.resolve("rdt-archive-" + date + ".jsonl.gz");
        Path staging = null;
        try {
            Files.createDirectories(directory);
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Archive already exists: " + target);
            }
            staging = Files.createTempFile(directory, ".rdt-archive-" + date + "-", ".tmp");
            temporary = staging;
            writer = openWriter(staging);
        } catch (IOException failure) {
            if (staging != null) deleteAfterFailure(staging, failure);
            throw new RdtArchiveException("Cannot open local archive", failure);
        }
    }

    private static BufferedWriter openWriter(Path path) throws IOException {
        OutputStream output = Files.newOutputStream(path);
        try {
            return new BufferedWriter(new OutputStreamWriter(
                    new GZIPOutputStream(output, 64 * 1024, true), StandardCharsets.UTF_8), 64 * 1024);
        } catch (IOException | RuntimeException failure) {
            try { output.close(); } catch (IOException closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    @Override
    public void write(List<String> jsonRecords) {
        if (closed) throw new RdtArchiveException("Archive sink is closed");
        try {
            for (String record : jsonRecords) {
                // Raw newlines would split one JSON document into several JSONL records.
                if (record == null || record.isBlank() || record.indexOf('\n') >= 0 || record.indexOf('\r') >= 0) {
                    throw new RdtArchiveException("Archive records must be single-line JSON documents");
                }
                writer.write(record);
                writer.write('\n');
            }
            writer.flush();
        } catch (IOException failure) {
            abortAfterFailure(failure);
            throw new RdtArchiveException("Cannot write local archive", failure);
        } catch (RuntimeException failure) {
            abortAfterFailure(failure);
            throw failure;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            writer.close();
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            // A successful file is never replaced by a partial or repeated run.
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Archive already exists: " + target);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException failure) {
            deleteAfterFailure(temporary, failure);
            throw new RdtArchiveException("Cannot publish local archive", failure);
        }
        cleanExpiredFiles();
    }

    @Override
    public void abort() {
        if (closed) return;
        closed = true;
        IOException failure = null;
        try { writer.close(); } catch (IOException error) { failure = error; }
        try { Files.deleteIfExists(temporary); } catch (IOException error) {
            if (failure == null) failure = error; else failure.addSuppressed(error);
        }
        if (failure != null) throw new RdtArchiveException("Cannot discard local archive", failure);
    }

    private void abortAfterFailure(Throwable failure) {
        try { abort(); } catch (RuntimeException abortFailure) { failure.addSuppressed(abortFailure); }
    }

    private static void deleteAfterFailure(Path path, IOException failure) {
        try { Files.deleteIfExists(path); } catch (IOException deleteFailure) { failure.addSuppressed(deleteFailure); }
    }

    private void cleanExpiredFiles() {
        LocalDate cutoff = date.minusDays(retentionDays);
        try (var paths = Files.newDirectoryStream(target.getParent(), "rdt-archive-*.jsonl.gz")) {
            for (Path path : paths) {
                Matcher matcher = ARCHIVE_NAME.matcher(path.getFileName().toString());
                if (!matcher.matches() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) continue;
                try {
                    if (LocalDate.parse(matcher.group(1)).isBefore(cutoff)) Files.delete(path);
                } catch (DateTimeParseException ignored) {
                    // Unrecognized names are outside this archive's retention policy.
                } catch (IOException error) {
                    LOG.warn("Cannot remove expired archive {}", path, error);
                }
            }
        } catch (IOException error) {
            // Publication succeeded; a retention failure must not invalidate that archive.
            LOG.warn("Cannot inspect local archive retention in {}", target.getParent(), error);
        }
    }
}
