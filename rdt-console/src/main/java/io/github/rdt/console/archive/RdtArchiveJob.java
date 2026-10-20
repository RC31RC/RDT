package io.github.rdt.console.archive;

import java.time.Clock;
import java.time.LocalDate;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;

/** A single asynchronous, non-reentrant daily archive in this Console process. */
public class RdtArchiveJob {
    private static final Logger LOG = LoggerFactory.getLogger(RdtArchiveJob.class);
    private final Source reader;
    private final RdtArchiveSinkFactory sinks;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean();

    public RdtArchiveJob(Source reader, RdtArchiveSinkFactory sinks, Clock clock) {
        this.reader = reader;
        this.sinks = sinks;
        this.clock = clock;
    }

    @FunctionalInterface
    public interface Source {
        long read(Instant at, Consumer<List<String>> destination);
        default boolean available() { return true; }
    }

    @Async("rdtArchiveExecutor")
    @Scheduled(cron = "${rdt.console.archive.cron:0 0 2 * * ?}",
            zone = "${rdt.console.archive.zone:Asia/Shanghai}")
    public void archive() {
        if (!reader.available()) return;
        if (!running.compareAndSet(false, true)) {
            LOG.info("Skipping archive: previous run is still active");
            return;
        }
        RdtArchiveSink sink = null;
        try {
            var startedAt = clock.instant();
            LocalDate date = startedAt.atZone(clock.getZone()).toLocalDate();
            var opened = sinks.open(date);
            if (opened.isEmpty()) {
                LOG.info("Skipping archive: {} already completed", date);
                return;
            }
            sink = opened.get();
            long count = reader.read(startedAt, sink::write);
            sink.close();
            LOG.info("Archive completed for {}: {} records", date, count);
        } catch (RuntimeException failure) {
            if (sink != null) {
                try { sink.abort(); } catch (RuntimeException abortFailure) { failure.addSuppressed(abortFailure); }
            }
            LOG.error("Archive failed; no local archive will be published", failure);
        } finally {
            running.set(false);
        }
    }
}
