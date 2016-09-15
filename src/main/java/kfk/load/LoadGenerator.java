package kfk.load;

import com.cyngn.kafka.produce.MessageProducer;
import com.google.common.base.Strings;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class LoadGenerator implements Closeable {
    private final DeploymentOptions deploymentOptions;
    private final Vertx vertx;
    private final MessagePublisher publisher;
    private String producerDeploymentId;


    public LoadGenerator(final DeploymentOptions deploymentOptions, final Vertx vertx,
                         final MessagePublisher publisher) {
        this.deploymentOptions = deploymentOptions;
        this.vertx = vertx;
        this.publisher = publisher;
    }

    public void start() {
        vertx.deployVerticle(MessageProducer.class.getName(),
                deploymentOptions,
                deploy -> {
                    if (deploy.failed()) {
                        System.err.println(
                                String.format("Failed to start kafka producer verticle, ex: %s", deploy.cause()));
                        vertx.close();
                        return;
                    }
                    System.out.println("kafka producer verticle(s) started, id:" + deploy.result());
                    producerDeploymentId = deploy.result();

                    vertx.executeBlocking(f -> publisher.start(), r -> {
                    });
                });
    }

    @Override
    public void close() throws IOException {
        publisher.stop();
        if (!Strings.isNullOrEmpty(producerDeploymentId)) {
            vertx.undeploy(producerDeploymentId);
        }
    }
}
