package io.github.rdt.redisson.schedule;

import java.util.ArrayList;
import java.util.Set;
import org.redisson.api.RemoteInvocationOptions;
import org.redisson.codec.SerializationCodec;
import org.redisson.executor.params.TaskParameters;
import org.redisson.remote.RemoteServiceAck;
import org.redisson.remote.RemoteServiceRequest;
import org.redisson.remote.RemoteServiceResponse;

/** Restricted native transport envelope. Application objects are never accepted. */
final class JobCodec {
    private JobCodec() { }
    static SerializationCodec create() {
        return new SerializationCodec(JobInvocation.class.getClassLoader(), Set.of(
                JobInvocation.class.getName(), RemoteServiceRequest.class.getName(),
                RemoteServiceResponse.class.getName(), RemoteServiceAck.class.getName(),
                RemoteInvocationOptions.class.getName(), TaskParameters.class.getName(),
                String.class.getName(), Long.class.getName(), Number.class.getName(), Boolean.class.getName(),
                Object[].class.getName(), byte[].class.getName(), long[].class.getName(),
                RuntimeException.class.getName(), IllegalStateException.class.getName(),
                IllegalArgumentException.class.getName(), Exception.class.getName(), Throwable.class.getName(),
                StackTraceElement.class.getName(), StackTraceElement[].class.getName(),
                ArrayList.class.getName(), "java.util.Collections$EmptyList"));
    }
}
