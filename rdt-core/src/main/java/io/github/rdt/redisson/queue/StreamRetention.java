package io.github.rdt.redisson.queue;

import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.support.KeySpace;

import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

final class StreamRetention {
    private static final Logger log = LoggerFactory.getLogger(StreamRetention.class);
    private final QueuePolicy policy;
    private final Map<String, Long> lastTrim = new ConcurrentHashMap<>();

    StreamRetention(QueuePolicy policy, RedissonClient client, KeySpace keys) {
        this.policy = policy;
        this.client = client;
        this.keys = keys;
    }

    private final Map<String, CleanupRequest> dirty = new ConcurrentHashMap<>();
    private static final String DELETE_CONFIRMED =
            """
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups == 0 then return 0 end
local function compare(a, b)
  local am, as = string.match(a, '(%d+)%-(%d+)')
  local bm, bs = string.match(b, '(%d+)%-(%d+)')
  if #am ~= #bm then return #am < #bm end
  if am ~= bm then return am < bm end
  if #as ~= #bs then return #as < #bs end
  return as < bs
end
local delivered = {}; local pending = {}
for _, fields in ipairs(groups) do
  local name; local last
  for i = 1, #fields, 2 do
    if fields[i] == 'name' then name = fields[i + 1] end
    if fields[i] == 'last-delivered-id' then last = fields[i + 1] end
  end
  table.insert(delivered, last)
  local p = redis.call('XPENDING', KEYS[1], name)
  if p[1] > 0 then table.insert(pending, p[2]) end
end
local removed = 0
for _, id in ipairs(ARGV) do
  local safe = true
  for _, last in ipairs(delivered) do if compare(last, id) then safe = false; break end end
  if safe then
    for _, first in ipairs(pending) do
      if not compare(id, first) then safe = false; break end
    end
  end
  if safe then removed = removed + redis.call('XDEL', KEYS[1], id) end
end
return removed
""";
    private final RedissonClient client;
    private final KeySpace keys;
    private final AtomicLong revision = new AtomicLong();

    private record CleanupRequest(RStream<String, String> stream, long revision) {}

    /** Local bookkeeping only; group checks and deletion belong to the maintenance loop. */
    void markAcknowledged(QueuePolicy effectivePolicy, RStream<String, String> stream) {
        if (effectivePolicy.autoDeleteAfterAck()) {
            // ACK stays cheap. Group watermarks are checked once per bounded maintenance batch.
            dirty.put(stream.getName(), new CleanupRequest(stream, revision.incrementAndGet()));
        }
    }

    void sweepAcknowledged() {
        dirty.forEach(
                (name, request) -> {
                    var stream = request.stream();
                    try {
                        if (!maintenanceTurn(name, Duration.ofSeconds(1))) return;
                        List<StreamMessageId> ids =
                                new ArrayList<>(
                                        stream.range(256, StreamMessageId.MIN, StreamMessageId.MAX)
                                                .keySet());
                        int removed = deleteConfirmed(stream, ids);
                        if (ids.size() < 256 && removed == ids.size()) dirty.remove(name, request);
                    } catch (RuntimeException error) {
                        log.warn("Acknowledged Stream cleanup failed stream={}", name, error);
                    }
                });
    }

    private boolean maintenanceTurn(String name, Duration interval) {
        return client.getBucket(keys.key("stream-maintenance", name), StringCodec.INSTANCE)
                .setIfAbsent("1", interval);
    }

    private int deleteConfirmed(RStream<String, String> stream, List<StreamMessageId> candidates) {
        if (candidates.isEmpty()) return 0;
        Number removed =
                client.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                DELETE_CONFIRMED,
                                RScript.ReturnType.LONG,
                                List.of(stream.getName()),
                                candidates.stream().map(StreamMessageId::toString).toArray());
        return removed.intValue();
    }

    /**
     * 在消息成功写入后清理已经完成生命周期的 Stream 原文。
     *
     * <p>不能直接使用 Redis 的 MAXLEN/MINID 裁剪，因为这些命令不理解消费组： 它们可能删除尚未投递或仍处于 Pending 状态的消息。这里仅扫描保留边界内的
     * 旧消息，并按批次校验所有消费组的已投递和 Pending 边界。这样在可靠广播存在离线 实例时，消息仍然可以等到该实例恢复后消费。
     *
     * <p>清理失败不会回滚已经成功的消息投递；下一次发送会再次尝试，管理端仍可 观察并手动处理当前 Stream 状态。
     */
    void trimStream(RStream<String, String> stream, String destination) {
        if (policy.streamMaxLength() == 0 && policy.streamRetention().isZero()) {
            return;
        }
        long now = System.currentTimeMillis();
        Long previous = lastTrim.putIfAbsent(destination, now);
        if (previous != null
                && (now - previous < 30000 || !lastTrim.replace(destination, previous, now)))
            return;
        try {
            if (!maintenanceTurn("retention:" + stream.getName(), Duration.ofSeconds(30))) return;
            List<StreamMessageId> candidates = retentionCandidates(stream);
            deleteConfirmed(stream, candidates);
        } catch (RuntimeException e) {
            log.warn("Stream retention trim failed destination={}", destination, e);
        }
    }

    /**
     * 返回可能超过时间或数量边界的旧消息。
     *
     * <p>数量边界只决定扫描范围，不直接删除消息。若 Stream 中存在 Pending 消息，批次边界检查会保护它；若离线消费组尚未投递，消费组游标检查也会保护它。
     */
    private List<StreamMessageId> retentionCandidates(RStream<String, String> stream) {
        int scanLimit = policy.streamMaxLength() > 0 ? 64 : 1000;
        List<StreamMessageId> candidates = new ArrayList<>();
        if (!policy.streamRetention().isZero()) {
            long minimumTimestamp =
                    Math.max(0L, System.currentTimeMillis() - policy.streamRetention().toMillis());
            candidates.addAll(
                    stream.range(
                                    64,
                                    new StreamMessageId(0, 0),
                                    new StreamMessageId(minimumTimestamp, 0))
                            .keySet());
        }
        if (policy.streamMaxLength() > 0) {
            long size = stream.size();
            if (size > policy.streamMaxLength()) {
                int excess = (int) Math.min(size - policy.streamMaxLength(), scanLimit);
                candidates.addAll(
                        stream.range(excess, StreamMessageId.MIN, StreamMessageId.MAX).keySet());
            }
        }
        return candidates.stream().distinct().limit(scanLimit).toList();
    }
}
