package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConsistencyException;
import java.util.function.Supplier;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

/** Translate JDBC/transaction infrastructure only; never translate business callback failures. */
final class JdbcSupport {
    private JdbcSupport() { }

    static <T> T execute(String operation, Supplier<T> work) {
        try {
            return work.get();
        } catch (DataAccessException | TransactionException failure) {
            throw new RdtConsistencyException(operation + " failed", failure);
        }
    }

    static void execute(String operation, Runnable work) {
        execute(operation, () -> { work.run(); return null; });
    }
}
