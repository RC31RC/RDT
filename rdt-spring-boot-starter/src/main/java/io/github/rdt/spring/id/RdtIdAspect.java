package io.github.rdt.spring.id;

import io.github.rdt.id.DistributedIdGenerator;
import io.github.rdt.id.IdMode;
import io.github.rdt.id.RdtId;
import io.github.rdt.spring.support.RdtMethodSupport;
import io.github.rdt.support.RdtSupport;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;

@Aspect
@Order(-100)
public final class RdtIdAspect {
    private final DistributedIdGenerator ids;
    private final RdtMethodSupport methods;

    public RdtIdAspect(DistributedIdGenerator ids, RdtMethodSupport methods) {
        this.ids = ids;
        this.methods = methods;
    }

    @Around("@annotation(annotation)")
    public Object allocate(ProceedingJoinPoint point, RdtId annotation) throws Throwable {
        var method = methods.method(point);
        methods.validate(method);
        RdtSupport.require(method.getReturnType() == long.class || method.getReturnType() == Long.class,
                "@RdtId requires a long/Long return type");
        RdtSupport.require(annotation.minimum().isBlank() || annotation.mode() == IdMode.ATOMIC,
                "@RdtId minimum requires ATOMIC mode");
        Object result = point.proceed();
        String sequence = methods.key(annotation.sequence(), method, point.getTarget(), point.getArgs(), result);
        if (annotation.minimum().isBlank()) return ids.next(sequence, annotation.mode());
        Object minimum = methods.evaluate(annotation.minimum(), method, point.getTarget(), point.getArgs(), result);
        RdtSupport.require(minimum instanceof Long || minimum instanceof Integer,
                "@RdtId minimum must be an integer");
        return ids.nextAtLeast(sequence, ((Number) minimum).longValue());
    }
}
