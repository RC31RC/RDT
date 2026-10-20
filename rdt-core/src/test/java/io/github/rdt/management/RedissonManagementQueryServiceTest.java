package io.github.rdt.management;

import io.github.rdt.redisson.management.RedissonManagementQueryService;

import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.api.RScript;
import org.redisson.api.RKeys;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamGroup;
import org.redisson.client.codec.StringCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RedissonManagementQueryServiceTest {
    private final RedissonClient client = mock(RedissonClient.class);
    private final RKeys redisKeys = mock(RKeys.class);
    private final KeySpace keys = new KeySpace("overview:test");
    private final JsonCodec json = new JsonCodec();

    @BeforeEach
    void scripts() {
        when(client.getScript(StringCodec.INSTANCE)).thenReturn(mock(RScript.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void readsDisplayNameWithoutLoadingAttempts() {
        when(client.getKeys()).thenReturn(redisKeys);
        String key = keys.key("schedule-state", "poll-v1");
        when(redisKeys.countExists(key)).thenReturn(1L);
        RMap<String, String> state = mock(RMap.class);
        when(client.<String, String>getMap(key, StringCodec.INSTANCE)).thenReturn(state);
        when(state.readAllMap()).thenReturn(Map.of("next", "1000", "definition", "PT2M",
                "displayName", "Warframe世界状态轮询"));
        var queries = new RedissonManagementQueryService(client, keys, json);
        assertThat(queries.task("poll-v1").orElseThrow().displayName()).isEqualTo("Warframe世界状态轮询");
    }

    @Test
    @SuppressWarnings("unchecked")
    void retiredSnapshotIsIgnoredEvenIfTheIndexWasReadBeforeRetirement() {
        when(client.getKeys()).thenReturn(redisKeys);
        String key = keys.key("schedule-state", "retired-job");
        when(redisKeys.countExists(key)).thenReturn(1L);
        RMap<String, String> state = mock(RMap.class);
        when(client.<String, String>getMap(key, StringCodec.INSTANCE)).thenReturn(state);
        when(state.readAllMap()).thenReturn(Map.of("retired", "true"));
        var tasks = index("tasks");
        when(tasks.valueRange(0, 9)).thenReturn(List.of("retired-job"));
        var queries = new RedissonManagementQueryService(client, keys, json);
        assertThat(queries.task("retired-job")).isEmpty();
        assertThat(queries.tasks(0, 10).items()).isEmpty();
    }

    @Test
    void emptyNamespaceHasZeroCountsWithoutKeyspaceScan() {
        index("streams");
        index("tasks");
        index("dead-letters");
        index("dead-letters-expiry");
        assertThat(new RedissonManagementQueryService(client, keys, json).overview())
                .isEqualTo(new ManagementOverview(keys.namespace(), 0, 0, 0, 0, 0, 0, 0, false));
        verifyNoInteractions(redisKeys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void overviewCountsIndexedResourcesWithoutReadingPayloads() {
        var streams = index("streams");
        var tasks = index("tasks");
        var failures = index("failures:resource");
        var deadLetters = index("dead-letters");
        index("failures:resource-expiry");
        index("dead-letters-expiry");
        when(streams.size()).thenReturn(1);
        when(tasks.size()).thenReturn(1);
        when(failures.size()).thenReturn(1);
        when(deadLetters.size()).thenReturn(25);
        when(streams.valueRange(0, 199)).thenReturn(List.of("resource"));
        when(tasks.valueRange(0, 199)).thenReturn(List.of("resource"));
        RStream<String, String> stream = mock(RStream.class);
        StreamGroup group = mock(StreamGroup.class);
        when(client.<String, String>getStream(keys.key("stream", "resource"), StringCodec.INSTANCE)).thenReturn(stream);
        when(stream.isExists()).thenReturn(true);
        when(stream.size()).thenReturn(7L);
        when(stream.listGroups()).thenReturn(List.of(group));
        when(group.getPending()).thenReturn(2);
        assertThat(new RedissonManagementQueryService(client, keys, json).overview())
                .isEqualTo(new ManagementOverview(keys.namespace(), 1, 7, 1, 2, 1, 1, 25, false));
        verify(stream).size();
        verify(stream).listGroups();
        verifyNoInteractions(redisKeys);
    }

    @SuppressWarnings("unchecked")
    private RScoredSortedSet<String> index(String kind) {
        RScoredSortedSet<String> index = mock(RScoredSortedSet.class);
        when(client.<String>getScoredSortedSet(keys.key(kind.endsWith("-expiry") ? "management-expiry" : "management-index",kind.endsWith("-expiry") ? kind.substring(0,kind.length()-7) : kind), StringCodec.INSTANCE)).thenReturn(index);
        when(index.getName()).thenReturn(keys.key(kind.endsWith("-expiry") ? "management-expiry" : "management-index", kind.endsWith("-expiry") ? kind.substring(0, kind.length()-7) : kind));
        return index;
    }
}
