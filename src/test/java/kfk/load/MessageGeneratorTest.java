package kfk.load;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MessageGeneratorTest {
    @Test
    public void messageSizeMustBeBearApproxSize() throws Exception {

        int[] sizes = {100, 1000, 1024, 2 * 1024};

        for (int size : sizes) {
            final MessageGenerator generator = new MessageGenerator(size);
            generator.reset();
            final int expectedSize = (int) (1.2 * size);
            for (int i = 0; i < 1000; i++) {
                final String message = generator.nextMessage();
                Assert.assertThat("Message: " + message + " size must be less than: " + expectedSize,
                        message.length(), Matchers.lessThanOrEqualTo(expectedSize));
            }
        }

    }
}
