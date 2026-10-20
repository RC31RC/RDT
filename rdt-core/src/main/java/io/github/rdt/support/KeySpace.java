package io.github.rdt.support;

import java.util.Optional;

/** Slot groups are the connected components of multi-key scripts, not the whole application. */
public record KeySpace(String namespace) {
    public KeySpace { RdtSupport.component(namespace); }

    public String key(String feature, String name) {
        String encoded=RdtSupport.component(name);
        String tag=RdtSupport.component(namespace)+"-"+RdtSupport.component(slotGroup(feature,name));
        return featurePrefix(feature)+":{rdt-"+tag+"}:"+encoded;
    }

    public String featurePrefix(String feature) {
        RdtSupport.require(feature!=null && !feature.isBlank() && !feature.contains(":"), "Invalid key feature");
        return "rdt:"+RdtSupport.component(namespace)+":"+feature;
    }
    public String pattern(String feature) { return featurePrefix(feature)+":*"; }

    public Optional<String> nameFromKey(String feature,String actualKey) {
        String prefix=featurePrefix(feature)+":{rdt-";
        if(actualKey==null || !actualKey.startsWith(prefix))return Optional.empty();
        int end=actualKey.indexOf("}:",prefix.length());
        if(end<0)return Optional.empty();
        return RdtSupport.decodeComponent(actualKey.substring(end+2))
                .filter(name -> key(feature,name).equals(actualKey));
    }

    private String slotGroup(String feature,String name) {
        if(feature.equals("stream") || feature.equals("message-delay"))return "queue:"+name;
        if(feature.equals("message-retries")) {
            int split=name.indexOf(':');
            RdtSupport.require(split>0,"Invalid queue retry identity");
            return "queue:"+RdtSupport.decodeComponent(name.substring(0,split)).orElseThrow();
        }
        if(feature.equals("owner") && name.startsWith("node:"))return name;
        if(feature.equals("runtime-instance")) {
            int split=name.lastIndexOf(':');
            return "node:"+(split>0 ? name.substring(0,split) : name);
        }
        if(feature.startsWith("schedule-") && !feature.equals("schedule-due"))return "job:"+name;
        if((feature.equals("management-index") || feature.equals("management-expiry")) && (name.startsWith("attempts:") || name.startsWith("failures:"))) {
            String job=name.substring(name.indexOf(':')+1);
            return "job:"+job;
        }
        // Single-key scripts and all Redisson auxiliary keys retain their own independent tag.
        return feature+":"+name;
    }
}
