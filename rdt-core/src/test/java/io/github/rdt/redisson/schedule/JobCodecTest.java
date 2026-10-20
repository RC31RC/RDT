package io.github.rdt.redisson.schedule;

import io.github.rdt.exception.RdtException;
import io.github.rdt.support.JsonCodec;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobCodecTest {
    @Test
    void workerProtocolIsDataOnlyJsonAndValidatesFields() {
        var codec = new JsonCodec();
        var invocation = new JobInvocation("job", "execution", "token");
        String encoded = codec.write(invocation);
        assertThat(encoded).doesNotContain("@class", "handler");
        assertThat(codec.<JobInvocation>read(encoded, JobInvocation.class)).isEqualTo(invocation);
        assertThat(Runnable.class.isAssignableFrom(JobInvocation.class)).isFalse();
        assertThatThrownBy(() -> new JobInvocation("", "execution", "token")).isInstanceOf(RdtException.class);
    }
}
