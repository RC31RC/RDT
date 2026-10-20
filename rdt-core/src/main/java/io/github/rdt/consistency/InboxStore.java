package io.github.rdt.consistency;

/** Must join the caller's business transaction. False means already committed by this group. */
public interface InboxStore { boolean register(InboxScope scope); }
