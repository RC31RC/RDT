package io.github.rdt.spring.queue;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.consistency.OutboxAppend;
import io.github.rdt.consistency.OutboxStore;
import io.github.rdt.exception.RdtException;
import java.util.Objects;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.RdtMessageSendManager;
import io.github.rdt.queue.MessageDescriptor;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.queue.PublishMode;
import io.github.rdt.queue.PublishReceipt;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Resolves optional collaborators on invocation, after all auto-configurations have been registered. */
public final class DefaultMessageSendManager implements RdtMessageSendManager {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageSendManager.class);
    private final ObjectProvider<MessageQueueTemplate> transports;
    private final ObjectProvider<OutboxStore> stores;
    private final JsonCodec json;
    private final KeySpace keys;
    public DefaultMessageSendManager(ObjectProvider<MessageQueueTemplate> transports, ObjectProvider<OutboxStore> stores,
                                   JsonCodec json, KeySpace keys) {
        this.transports = transports; this.stores = stores; this.json = json; this.keys = keys;
    }
    @Override
    public PublishReceipt send(MessageDescriptor<?> descriptor) { return publish(descriptor, PublishMode.DIRECT); }

    @Override
    public PublishReceipt sendAfterCommit(MessageDescriptor<?> descriptor) { return publish(descriptor, PublishMode.AFTER_COMMIT); }

    @Override
    public PublishReceipt sendOutbox(MessageDescriptor<?> descriptor) { return publish(descriptor, PublishMode.OUTBOX); }

    private PublishReceipt publish(MessageDescriptor<?> descriptor, PublishMode mode) {
        Objects.requireNonNull(descriptor, "message descriptor");
        String topic = descriptor.topic();
        RdtSupport.component(topic);
        QueueMode queueMode = Objects.requireNonNull(descriptor.mode(), "message mode");
        Message message = descriptor.envelope(json);
        if (mode == PublishMode.OUTBOX) {
            OutboxStore store = stores.getIfAvailable();
            if (store == null) throw configuration("OUTBOX requires rdt-consistency-jdbc-starter and rdt.consistency.enabled=true");
            if (!TransactionSynchronizationManager.isActualTransactionActive()
                    || TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
                throw configuration("OUTBOX requires an active writable application transaction (@Transactional)");
            }
            if (queueMode == QueueMode.BROADCAST) throw configuration("OUTBOX requires durable STREAM or RELIABLE_BROADCAST transport");
            String id = store.append(new OutboxAppend(keys.namespace(), topic, queueMode, message));
            return new PublishReceipt(message.id(), mode, PublishReceipt.Status.OUTBOX_STORED, id);
        }
        MessageQueueTemplate transport = transports.getIfAvailable();
        if (transport == null) throw configuration("Publishing requires rdt.queue.enabled=true and MessageQueueTemplate");
        if (mode == PublishMode.DIRECT) {
            transport.sendMessage(topic, message, queueMode);
            return new PublishReceipt(message.id(), mode, PublishReceipt.Status.SENT, null);
        }
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            throw configuration("AFTER_COMMIT requires an active application transaction; use DIRECT outside transactions");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try { transport.sendMessage(topic, message, queueMode); }
                catch (RuntimeException error) { log.error("Best-effort AFTER_COMMIT delivery failed destination={} messageId={}", topic, message.id(), error); }
            }
        });
        return new PublishReceipt(message.id(), mode, PublishReceipt.Status.AFTER_COMMIT_REGISTERED, null);
    }
    private RdtException configuration(String message) { return new RdtConfigurationException(message); }
}
