package kfk.load;

import com.cyngn.kafka.produce.KafkaPublisher;
import com.google.common.util.concurrent.RateLimiter;
import io.vertx.core.eventbus.EventBus;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class MessagePublisher {
    private final MessageGenerator generator;
    private final String topic;
    private final AtomicBoolean publishing = new AtomicBoolean(true);

    private final EventBus eventBus;
    private final RateLimiter rateLimiter;

    public MessagePublisher(final MessageGenerator generator, final String topic, final EventBus eventBus,
                            final RateLimiter rateLimiter) {
        this.generator = generator;
        this.topic = topic;
        this.eventBus = eventBus;
        this.rateLimiter = rateLimiter;
    }

    public void start() {
        publishing.set(true);
        System.out.println("Starting publisher:" + rateLimiter);
        KafkaPublisher publisher = new KafkaPublisher(eventBus);
        while (publishing.get()) {
            rateLimiter.acquire();
            publisher.send(topic, generator.nextMessage());
        }
        System.out.println("Publisher stopped.");
    }

    public void stop() {
        publishing.set(false);
    }
}
