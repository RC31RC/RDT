#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
JAR=${RDT_CONSOLE_JAR:-"$ROOT/rdt-console/target/rdt-console-1.0.0-SNAPSHOT.jar"}
OPTIONS=${RDT_CONSOLE_JVM_OPTIONS:-"$ROOT/rdt-console/jvm.options"}
JAVA=java
if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin/java"
fi

if [ ! -f "$JAR" ]; then
    printf '%s\n' "Console JAR not found: $JAR" \
        "Build it with: sh mvnw -pl rdt-console -am package" >&2
    exit 1
fi

if [ ! -f "$OPTIONS" ]; then
    printf '%s\n' "Console JVM options not found: $OPTIONS" >&2
    exit 1
fi

set -- -jar "$JAR" "$@"
if [ -n "${RDT_CONSOLE_HEAP_MAX:-}" ]; then
    set -- "-Xmx${RDT_CONSOLE_HEAP_MAX}" "$@"
fi
if [ -n "${RDT_CONSOLE_HEAP_INITIAL:-}" ]; then
    set -- "-Xms${RDT_CONSOLE_HEAP_INITIAL}" "$@"
fi
exec "$JAVA" "@$OPTIONS" "$@"
