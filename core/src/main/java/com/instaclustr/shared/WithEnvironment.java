package com.instaclustr.shared;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/** Utility class to temporarily set environment variables for the duration of a try-with-resources block.
 * Important: not a thread-safe.
 */
public class WithEnvironment implements AutoCloseable{
    // Holds properties that are added to the System.Environment for the lifetime of this object.
    private final Map<String, String> properties = new HashMap<>();

    /** Constructor that sets environment variables in key-value pairs.
     * If keyValues is null, no environment variables are set.
     * @param keyValues Key-value pairs of environment variables to set.
     * @throws IllegalArgumentException if keyValues is not in pairs.
     */
    public WithEnvironment(String ...keyValues) {
        set(keyValues);
    }

    /** Sets environment variables in key-value pairs.
     * If keyValues is null, no environment variables are set.
     * @param keyValues Key-value pairs of environment variables to set.
     * @throws IllegalArgumentException if keyValues is not in pairs.
     */
    public void set(String ...keyValues) throws IllegalArgumentException {
        if (keyValues == null || keyValues.length == 0) {
            return;
        }

        assert keyValues.length % 2 == 0 : "keyValues must be in pairs";

        try {
            Map<String, String> writableEnv = writableEnv();

            for (int i = 0; i < keyValues.length; i += 2) {
                this.properties.put(keyValues[i], keyValues[i + 1]);
                writableEnv.put(keyValues[i], keyValues[i + 1]);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set environment variables", e);
        }
    }

    /** Remove environment variables by keys. Only keys that were set by this object will be removed.
     * @param keys Keys of environment variables to remove.
     */
    public void remove(String ...keys) {
        if (keys == null || keys.length == 0) {
            return;
        }

        try {
            Map<String, String> writableEnv = writableEnv();

            for (String key : keys) {
                if (properties.containsKey(key)) {
                    properties.remove(key);
                    writableEnv.remove(key);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove environment variables", e);
        }
    }

    private Map<String, String> writableEnv() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> env = System.getenv();
        Class<?> cl = env.getClass();
        // It can potentially be broken in some jdk implementations if the name of the field in underlying UnmodifiableMap class is changed
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        return writableEnv;
    }

    @Override
    public void close() throws Exception {
        try {
            remove(properties.keySet().toArray(new String[0]));
        } catch (Exception e) {
            throw new RuntimeException("Failed to clear environment variables", e);
        }
    }
}
