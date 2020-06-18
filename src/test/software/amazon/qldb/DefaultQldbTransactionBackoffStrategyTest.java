package software.amazon.qldb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class DefaultQldbTransactionBackoffStrategyTest {

    @Test
    public void testNullBaseDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DefaultQldbTransactionBackoffStrategy(null, null);
        });
    }

    @Test
    public void testNullCapDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DefaultQldbTransactionBackoffStrategy(Duration.ofMillis(1), null);
        });
    }

    @Test
    public void testNegativeBaseDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DefaultQldbTransactionBackoffStrategy(Duration.ofMillis(-11), Duration.ofMillis(1));
        });
    }

    @Test
    public void testNegativeCapDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DefaultQldbTransactionBackoffStrategy(Duration.ofMillis(1), Duration.ofMillis(-1));
        });
    }

    @Test
    public void testBaseDurationLargerThanCapDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DefaultQldbTransactionBackoffStrategy(Duration.ofMillis(200), Duration.ofMillis(1));
        });
    }

    @Test
    public void testBaseDurationSameAsCapDuration() {
        assertDoesNotThrow(() -> {
            new DefaultQldbTransactionBackoffStrategy(Duration.ofMillis(1), Duration.ofMillis(1));
        });
    }
}
