package io.github.rdt.redisson.runtime;

import io.github.rdt.runtime.RuntimeDirectoryProvider;
import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.util.ArrayList;
import java.util.List;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

public final class RedisRuntimeDirectoryProvider implements RuntimeDirectoryProvider {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    public RedisRuntimeDirectoryProvider(RedissonClient client,KeySpace keys,JsonCodec json) {
        this.client=client;this.keys=keys;this.json=json;
    }
    @Override
    public List<RuntimeInstance> instances(long now) {
        var index=client.<String>getScoredSortedSet(keys.key("runtime-index","instances"),StringCodec.INSTANCE);
        List<RuntimeInstance> result=new ArrayList<>();
        for (int offset=0;;offset+=256) {
            var names=index.valueRange(now,true,Double.POSITIVE_INFINITY,true,offset,256);
            if (names.isEmpty()) break;
            String[] physical=names.stream().map(name -> keys.key("runtime-instance",name)).toArray(String[]::new);
            java.util.Map<String,String> values=client.getBuckets(StringCodec.INSTANCE).get(physical);
            List<RuntimeInstance> page=new ArrayList<>();
            var tokens=new java.util.HashMap<String,String>();
            for (String value:values.values()) {
                int separator=value.indexOf('\n');
                if (separator<0) continue;
                RuntimeInstance view=json.read(value.substring(separator+1),RuntimeInstance.class);
                if (view.lastHeartbeatAtMillis()+view.leaseMillis()<=now) continue;
                page.add(view); tokens.put(view.instanceId()+":"+view.runId(),value.substring(0,separator));
            }
            if (!page.isEmpty()) {
                var owners=client.<String>getBuckets(StringCodec.INSTANCE).get(page.stream()
                        .map(view -> keys.key("owner","node:"+view.instanceId())).toArray(String[]::new));
                for (var view:page) if (tokens.get(view.instanceId()+":"+view.runId()).equals(owners.get(keys.key("owner","node:"+view.instanceId())))) result.add(view);
            }
            if (names.size()<256) break;
        }
        return List.copyOf(result);
    }
}
