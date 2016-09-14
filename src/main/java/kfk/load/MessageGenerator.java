package kfk.load;

import com.google.common.base.Strings;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class MessageGenerator {
    private final AtomicLong counter = new AtomicLong();
    private final int approxSizeBytes;

    public MessageGenerator(final int approxSizeBytes) {
        this.approxSizeBytes = approxSizeBytes;
    }

    public String nextMessage() {
        final long index = counter.incrementAndGet();
        final String segment = Long.toHexString(index);
        return Strings.repeat(segment, approxSizeBytes / segment.length()) + "-" + index;
    }

    public void reset() {
        counter.set(0);
    }
}
