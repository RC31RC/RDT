package io.github.rdt.console.archive;

import java.time.LocalDate;
import java.util.Optional;

/** Creates a fresh sink for each run; empty means this date is already archived. */
@FunctionalInterface
public interface RdtArchiveSinkFactory {
    Optional<RdtArchiveSink> open(LocalDate date);
}
