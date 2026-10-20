package io.github.rdt.runtime;

/** A bounded snapshot of a locally registered handler, without payloads or credentials. */
public record RuntimeComponent(String kind, String id, String destination, String group,
                               String definition, int capacity, int active, int buffered, int bufferCapacity) { }
