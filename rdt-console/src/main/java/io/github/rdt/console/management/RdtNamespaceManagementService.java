package io.github.rdt.console.management;

import io.github.rdt.management.ManagementCommandService;
import io.github.rdt.management.ManagementQueryService;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.management.RedissonManagementCommandService;
import io.github.rdt.redisson.management.RedissonManagementQueryService;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;

/** 发现并限制可管理 namespace，按请求创建对应的查询和命令服务。 */
public final class RdtNamespaceManagementService {
    private static final String RDT_PREFIX = "rdt:";
    private final RedissonClient client;
    private final List<String> configuredNamespaces;
    // JsonCodec is immutable and thread-safe; requests must not rebuild Jackson module discovery.
    private static final JsonCodec JSON = new JsonCodec();

    /**
     * @param client Redis 客户端
     * @param configuredNamespaces 显式允许的 namespace；为空时自动发现
     */
    public RdtNamespaceManagementService(RedissonClient client, List<String> configuredNamespaces) {
        this.client = client;
        this.configuredNamespaces = List.copyOf(configuredNamespaces);
    }

    /** @return 当前可选择的 namespace */
    public List<String> namespaces() {
        if (!configuredNamespaces.isEmpty()) {
            return configuredNamespaces.stream().sorted().toList();
        }
        Set<String> namespaces = new TreeSet<>();
        var scan = KeysScanOptions.defaults().pattern("rdt:*").chunkSize(200);
        for (String key : client.getKeys().getKeys(scan)) {
            namespaceFromKey(key).ifPresent(namespaces::add);
        }
        return List.copyOf(namespaces);
    }

    /** @param namespace 目标 namespace @return 只读查询服务 */
    public ManagementQueryService queries(String namespace) {
        KeySpace keys = keys(namespace);
        return new RedissonManagementQueryService(client, keys, JSON);
    }

    /** @param namespace 目标 namespace @return 受限命令服务 */
    public ManagementCommandService commands(String namespace) {
        KeySpace keys = keys(namespace);
        return new RedissonManagementCommandService(client, keys, new RedissonLockTemplate(client, keys), JSON);
    }

    private KeySpace keys(String namespace) {
        String encodedNamespace = RdtSupport.component(namespace);
        boolean available = configuredNamespaces.isEmpty()
                ? client.getKeys().getKeys(KeysScanOptions.defaults()
                        .pattern(RDT_PREFIX + encodedNamespace + ":*").chunkSize(100).limit(1))
                        .iterator().hasNext()
                : configuredNamespaces.contains(namespace);
        RdtSupport.require(available, "Namespace is not available to this console");
        return new KeySpace(namespace);
    }

    private Optional<String> namespaceFromKey(String key) {
        if (key == null || !key.startsWith(RDT_PREFIX)) {
            return Optional.empty();
        }
        int namespaceEnd = key.indexOf(':', RDT_PREFIX.length());
        if (namespaceEnd < 0) {
            return Optional.empty();
        }
        String encodedNamespace = key.substring(RDT_PREFIX.length(), namespaceEnd);
        return RdtSupport.decodeComponent(encodedNamespace)
                .filter(namespace -> RdtSupport.component(namespace).equals(encodedNamespace));
    }
}
