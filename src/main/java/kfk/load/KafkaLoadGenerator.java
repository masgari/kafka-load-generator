package kfk.load;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Optional;
import java.util.function.Consumer;

/**
 */
public class KafkaLoadGenerator {
    public static void main(String[] arguments) {
        final Consumer<Exception> exceptionHandler = KafkaLoadGenerator::handleException;
        final Consumer<Integer> exitHandler = System::exit;

        final Args args = new Args();
        final JCommander parser = new JCommander(args);
        try {
            parser.parse(arguments);
        } catch (ParameterException e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Parameter error: " + e.getMessage()));
        } catch (Exception e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Unknown error: " + e.getMessage()));
        }

        try {
            args.verify();
        } catch (Exception e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Verification failed: " + e.getMessage()));
        }

        if (args.help()) {
            printUsageAndExit(parser, exitHandler, Optional.empty());
        }

        KafkaLoadGenerator klg = new KafkaLoadGenerator();
        klg.run(exceptionHandler, args);
    }

    void run(final Consumer<Exception> exceptionHandler, final Args args) {
        final Vertx vertx = Vertx.vertx();
        final DeploymentOptions options = createDeploymentOptions(args);
        final RateLimiter rateLimiter = RateLimiter.create(args.rate());
        final MessageGenerator generator = new MessageGenerator(args.messageSize());
        final MessagePublisher
                publisher = new MessagePublisher(generator, args.topic(), vertx.eventBus(), rateLimiter);

        try (final LoadGenerator loadGenerator = new LoadGenerator(options, vertx, publisher)) {
            loadGenerator.start();
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }

    DeploymentOptions createDeploymentOptions(final Args args) {
        JsonObject producerConfig = new JsonObject();
        producerConfig.put("bootstrap.servers", args.kafkaCluster().toString());
        producerConfig.put("serializer.class", args.messageSerializerClass());
        producerConfig.put("default_topic", args.topic());

        return new DeploymentOptions()
                .setInstances(args.threads())
                .setConfig(producerConfig);
    }


    private static void printUsageAndExit(JCommander parser, Consumer<Integer> exitHandler,
                                          Optional<String> optionalMessage) {
        if (optionalMessage.isPresent()) {
            JCommander.getConsole().println(optionalMessage.get());
        }
        parser.usage();
        exitHandler.accept(1);
    }

    private static void handleException(final Exception ex) {
        ex.printStackTrace();
    }

}
