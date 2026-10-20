package io.github.rdt.console.security;

import io.github.rdt.exception.RdtConfigurationException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/** PBKDF2-HMAC-SHA256, per-password salt; no plaintext or reversible user passwords. */
public final class ConsolePasswords {
    private static final int ITERATIONS = 600_000;
    private final SecureRandom random = new SecureRandom();

    public String hash(String password) {
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        return "pbkdf2-sha256$" + ITERATIONS + "$" + encode(salt) + "$" + encode(derive(password, salt));
    }

    public boolean matches(String password, String stored) {
        if (password == null || password.length() > 128) return false;
        String[] parts = stored.split("\\$", -1);
        if (parts.length != 4 || !parts[0].equals("pbkdf2-sha256") || !parts[1].equals(Integer.toString(ITERATIONS))) {
            throw new RdtConfigurationException("Unsupported Console password hash format");
        }
        return MessageDigest.isEqual(derive(password, Base64.getUrlDecoder().decode(parts[2])), Base64.getUrlDecoder().decode(parts[3]));
    }

    private byte[] derive(String password, byte[] salt) {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, 256);
        try { return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded(); }
        catch (GeneralSecurityException failure) { throw new RdtConfigurationException("Console password hashing is unavailable", failure); }
        finally { spec.clearPassword(); }
    }

    private static String encode(byte[] bytes) { return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
}
