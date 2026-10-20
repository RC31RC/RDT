package io.github.rdt.spring.support;

import org.springframework.expression.ExpressionException;
import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtExpressionException;
import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.support.RdtSupport;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.reactivestreams.Publisher;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/** 共享的代理方法解析、严格 SpEL 键解析及同步调用契约校验。 */
public final class RdtMethodSupport implements EnvironmentAware {
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final DefaultParameterNameDiscoverer names = new DefaultParameterNameDiscoverer();
    private Environment environment = new StandardEnvironment();

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public String resolve(String value) {
        try {
            return environment.resolveRequiredPlaceholders(value);
        } catch (IllegalArgumentException failure) {
            throw new RdtConfigurationException("Cannot resolve RDT property: " + value, failure);
        }
    }

    /** 创建方法调用基础设施，支持参数位置和参数名称。 */
    public RdtMethodSupport() { }

    /**
     * 解析 Spring 代理背后的实现方法。
     * @param joinPoint 被拦截的方法调用
     * @return 解析桥接后的实现方法
     */
    public Method method(ProceedingJoinPoint joinPoint) {
        Method signature = ((MethodSignature) joinPoint.getSignature()).getMethod();
        return BridgeMethodResolver.findBridgedMethod(AopUtils.getMostSpecificMethod(signature,
                AopProxyUtils.ultimateTargetClass(joinPoint.getTarget())));
    }

    /**
     * 计算非空键，支持 #p0、#a0、参数名称和 #result。
     * 表达式属于应用代码，不得来自不可信的请求字符串。
     * @param expression SpEL 表达式
     * @param method 已解析的方法
     * @param target 调用目标对象
     * @param args 调用参数
     * @param result 成功结果或 null
     * @return 字符串键
     */
    public String key(String expression, Method method, Object target, Object[] args, Object result) {
        Object value = evaluate(expression, method, target, args, result);
        RdtSupport.require(value != null && !value.toString().isBlank(), "SpEL key resolved to null or blank");
        return value.toString();
    }

    public Object evaluate(String expression, Method method, Object target, Object[] args, Object result) {
        String resolved = resolve(expression);
        try {
            MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(target, method, args, names);
            context.setVariable("result", result);
            return parser.parseExpression(resolved).getValue(context);
        } catch (ExpressionException e) {
            throw new RdtExpressionException("Cannot evaluate RDT key on " + method.toGenericString(), e);
        }
    }

    /**
     * 解析注解中的时长。
     * @param value ISO-8601、Spring Boot 简写时长或配置占位符；空字符串继承默认值
     * @param fallback 继承的默认时长，可以为 null
     * @return 解析后的时长
     */
    public Duration duration(String value, Duration fallback) {
        String resolved = resolve(value).trim();
        try {
            return resolved.isBlank() ? fallback
                    : DurationStyle.detectAndParse(resolved);
        } catch (IllegalArgumentException e) {
            throw new RdtConfigurationException("Invalid duration: " + value, e);
        }
    }

    /**
     * 拒绝不支持的公开方法契约，防止异步业务未完成时提前释放锁。
     * @param method 待校验的方法
     */
    public void validate(Method method) {
        Class<?> result = method.getReturnType();
        RdtSupport.require(Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers())
                && !Modifier.isFinal(method.getModifiers()), "RDT methods must be public, nonstatic and nonfinal: " + method);
        RdtSupport.require(!CompletionStage.class.isAssignableFrom(result) && !Future.class.isAssignableFrom(result)
                && !Publisher.class.isAssignableFrom(result), "RDT only supports synchronous methods: " + method);
    }

    /**
     * 通过原始 Bean 代理调用，不使用 setAccessible，并保留业务异常。
     * @param bean 包含增强器的 Spring Bean
     * @param method 公开的实现方法
     * @param args 调用参数
     * @return 调用结果
     * @throws Exception 原始受检业务异常
     */
    public Object invoke(Object bean, Method method, Object... args) throws Exception {
        try {
            return AopUtils.selectInvocableMethod(method, bean.getClass()).invoke(bean, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw new RdtQueueException("Invocation failed", e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new RdtConfigurationException("Method is not proxy-invocable: " + method, e);
        }
    }
}
