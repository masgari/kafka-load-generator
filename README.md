# kafka-load-generator
Kafka load generator, using Vert.x and Java8

### Building
```
./gradlew clean test iD
```

### Generating load
```
cd build/install/kafka-load-generator/bin
./kafka-load-generator -c localhost -t topic2 --size 8196 --rate 100000

./kafka-load-generator -h

```
