package io.github.rdt.schedule;

/** Worker routing is distinct from retry and misfire policy. */
public enum JobRoute { HEALTH_FIRST, RANDOM, SPECIFIED }
