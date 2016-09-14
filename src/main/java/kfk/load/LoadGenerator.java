package kfk.load;

import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class LoadGenerator implements Closeable {
    private final Args args;
    private final MessageGenerator meessageGenerator;
    private final Vertx vertx;

    public LoadGenerator(final Args args) {
        vertx = Vertx.vertx();
        this.args = args;
        this.meessageGenerator = new MessageGenerator(args.messageSize());
    }

    public void start() {
        final DeploymentOptions deploymentOptions = createDeploymentOptions();
        deployKafkaMessageProducers(deploymentOptions);
        startMessagePublishers();
    }

    private void startMessagePublishers() {
        KafkaPublisher publisher = new KafkaPublisher(vertx.eventBus());
        while (isRunning()) {
            publisher.send(args.topic(), meessageGenerator.nextMessage());
        }
    }

    private boolean isRunning() {
        return true;
    }

    private DeploymentOptions createDeploymentOptions() {
        // sample config
        JsonObject producerConfig = new JsonObject();
        producerConfig.put("bootstrap.servers", args.kafkaCluster().toString());
        producerConfig.put("serializer.class", args.messageSerializerClass());
        producerConfig.put("default_topic", args.topic());

        return new DeploymentOptions()
                .setInstances(args.threads())
                .setConfig(producerConfig);
    }

    @Override
    public void close() throws IOException {

    }

    void deployKafkaMessageProducers(final DeploymentOptions deploymentOptions) {
        // use your vertx reference to deploy the consumer verticle
        vertx.deployVerticle(MessageProducer.class.getName(),
                deploymentOptions,
                deploy -> {
                    if (deploy.failed()) {
                        System.err.println(
                                String.format("Failed to start kafka producer verticle, ex: %s", deploy.cause()));
                        vertx.close();
                        return;
                    }
                    System.out.println("kafka producer verticle started");
                });
    }
}
