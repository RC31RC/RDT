package io.github.rdt.console.alert;

/** No message payloads or credentials are included in an alert. */
public record DeadLetterAlert(String namespace, String level, long retainedCount, long recentCount,
                               long oldestAtMillis, long checkedAtMillis, String message) { }
