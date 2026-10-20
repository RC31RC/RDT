package io.github.rdt.spring.lease;

/** Revokes application resources before a lost node identity can be acquired again. */
@FunctionalInterface
public interface NodeIdentityListener {
    void identityLost();
}
