package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.consistency.ConsumerGuard;
import io.github.rdt.consistency.InboxScope;
import io.github.rdt.consistency.InboxStore;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcInboxGuard implements ConsumerGuard {
    private final InboxStore store;
    private final TransactionTemplate transaction;
    private final boolean enabled;
    public JdbcInboxGuard(InboxStore store, PlatformTransactionManager manager, boolean enabled) {
        this.store=store; this.enabled=enabled; transaction=new TransactionTemplate(manager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }
    @Override
    public boolean supportsInbox() { return true; }
    @Override
    public boolean enabledByDefault() { return enabled; }
    @Override
    public void consume(InboxScope scope, Message message, MessageHandler business) throws Exception {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new RdtConfigurationException("Inbox guard must own the outermost transaction; invoke it on the synchronous consumer thread");
        }
        try {
            JdbcSupport.execute("Inbox transaction", () -> transaction.executeWithoutResult(status -> {
                if (!store.register(scope)) return;
                try { business.handle(message); }
                catch (Exception error) { throw new CheckedBusinessFailure(error); }
            }));
        } catch (CheckedBusinessFailure error) { throw (Exception) error.getCause(); }
    }
    private static final class CheckedBusinessFailure extends RuntimeException {
        private CheckedBusinessFailure(Exception cause) { super(cause); }
    }
}
