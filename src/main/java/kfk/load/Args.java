package kfk.load;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;

/**
 */
public class Args {
    @Parameter(names = {"-h", "--help"}, description = "Display arguments usage help message", help = true)
    private boolean help;

    @Parameter(names = {"--threads"}, description = "Number of concurrent load generators")
    private int threads = Runtime.getRuntime().availableProcessors();

    @Parameter(names = {"--size"}, description = "Size of random message in kilo bytes")
    private int messageSize = 1024 * 2;

    @Parameter(names = {"-t", "--topic"}, description = "Name of topic")
    private String topic = "testTopic";

    @Parameter(names = {"-c", "--cluster"}, description = "Kafka cluster DNS entry", required = true)
    private String cluster;

    @Parameter(names = {"--port"}, description = "Kafka servers listening port")
    private int port = 9092;

    @Parameter(names = {"--rate"}, description = "Message publishing rate (per sec)")
    private double rate = 10e6 / 10;// 1 million message per sec

    @Parameter(names = {"--message-serializer"}, description = "Kafka message serializer class, must be in classpath")
    private String messageSerializerClass = "org.apache.kafka.common.serialization.StringSerializer";

    public int messageSize() {
        return messageSize;
    }

    public int threads() {
        return threads;
    }

    public String topic() {
        return topic;
    }

    public boolean help() {
        return help;
    }

    public double rate() {
        return rate;
    }

    public void verify() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cluster), "Invalid kafka cluster: %s", cluster);
        Preconditions.checkArgument(threads >= 1, "Invalid number of threads: %s", threads);
    }

    public String messageSerializerClass() {
        return messageSerializerClass;
    }

    public HostAndPort kafkaCluster() {
        return HostAndPort.fromParts(cluster, port);
    }
}
