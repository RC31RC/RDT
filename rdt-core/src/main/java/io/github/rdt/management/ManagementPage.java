package io.github.rdt.management;

import java.util.List;

/** Bounded, non-transactional management snapshot. */
public record ManagementPage<T>(List<T> items, int offset, int limit, long total) {
    public ManagementPage { items = List.copyOf(items); }
}
