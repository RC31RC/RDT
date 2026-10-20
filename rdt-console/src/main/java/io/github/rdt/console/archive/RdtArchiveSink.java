package io.github.rdt.console.archive;

import java.util.List;

/** One archive run. Implementations must not retain batches after write returns. */
public interface RdtArchiveSink extends AutoCloseable {
    /** Write complete JSON documents; each document becomes one JSONL line. */
    void write(List<String> jsonRecords);

    /** Finish and publish a successful archive. */
    @Override
    void close();

    /** Release resources without publishing an incomplete archive. */
    void abort();
}
