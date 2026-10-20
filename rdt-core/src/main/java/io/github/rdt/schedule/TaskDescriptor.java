package io.github.rdt.schedule;

/** Application-owned immutable task definition, obtained once before registration. */
public abstract class TaskDescriptor {
    public abstract ScheduleSpec schedule();
    public String displayName() { return ""; }
    public ScheduleOptions options() { return ScheduleOptions.defaults(); }
}
