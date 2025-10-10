package com.instaclustr.shared;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WithEnvironmentTest {

    @Test
    void testConstructor_setsEnvironmentVariables() {
        String key1 = "TEST_ENV_KEY1";
        String value1 = "TEST_ENV_VALUE1";
        String key2= "TEST_ENV_KEY2";
        String value2 = "TEST_ENV_VALUE2";

        try (WithEnvironment ignored = new WithEnvironment(key1, value1, key2, value2)) {
            assertEquals(value1, System.getenv(key1));
            assertEquals(value2, System.getenv(key2));
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        // After close, variable should be removed
        assertNull(System.getenv(key1));
        assertNull(System.getenv(key2));
    }

    @Test
    void testSet_setsEnvironmentVariables() {
        String key = "TEST_ENV_SET_KEY";
        String value = "TEST_ENV_SET_VALUE";
        WithEnvironment env = new WithEnvironment();
        env.set(key, value);
        assertEquals(value, System.getenv(key));
        try {
            env.close();
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        assertNull(System.getenv(key));
    }

    @Test
    void testTryResourceConstructorAndSet() {
        String key1 = "TEST_ENV_KEY1";
        String value1 = "TEST_ENV_VALUE1";
        String key2= "TEST_ENV_KEY2";
        String value2 = "TEST_ENV_VALUE2";

        try (final WithEnvironment env = new WithEnvironment(key1, value1)) {
            env.set(key2, value2);
            assertEquals(value1, System.getenv(key1));
            assertEquals(value2, System.getenv(key2));
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        assertNull(System.getenv(key1));
        assertNull(System.getenv(key2));
    }

    @Test
    void testSet_nullOrEmptyDoesNothing() {
        @SuppressWarnings("resource")
        WithEnvironment env = new WithEnvironment();
        assertDoesNotThrow(() -> env.set());
        assertDoesNotThrow(() -> env.set(new String[]{}));
    }

    @Test
    void testSet_oddNumberOfArgumentsThrowsAssertionError() {
        @SuppressWarnings("resource")
        WithEnvironment env = new WithEnvironment();
        assertThrows(AssertionError.class, () -> env.set("KEY_ONLY"));
    }

    @Test
    void testRemove() {
        String key1 = "TEST_ENV_REMOVE_KEY1";
        String value1 = "VALUE1";
        String key2 = "TEST_ENV_REMOVE_KEY2";
        String value2 = "VALUE2";
        WithEnvironment env = new WithEnvironment(key1, value1, key2, value2);
        assertEquals(value1, System.getenv(key1));
        assertEquals(value2, System.getenv(key2));
        env.remove(key1);
        assertNull(System.getenv(key1));
        assertEquals(value2, System.getenv(key2));
        env.remove(key2);
        assertNull(System.getenv(key2));
        try {
            env.close();
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
    }
}