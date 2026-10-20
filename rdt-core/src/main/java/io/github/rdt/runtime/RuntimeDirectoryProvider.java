package io.github.rdt.runtime;

import io.github.rdt.runtime.RuntimeInstance;
import java.util.List;

/** Full, TTL-validated routing directory, independent of management pagination and pruning. */
public interface RuntimeDirectoryProvider { List<RuntimeInstance> instances(long redisNow); }
