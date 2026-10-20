package io.github.rdt.schedule;

@FunctionalInterface
public interface ContextualTask { Object execute(TaskExecutionContext context) throws Exception; }
