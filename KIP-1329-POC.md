# KIP-1329 POC: ListClientConnections Admin API

## Summary

This branch contains a working proof-of-concept for [KIP-1329](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1329%3A+ListClientConnections+Admin+API) ([KAFKA-20526](https://issues.apache.org/jira/browse/KAFKA-20526)).

The POC adds a new `ListClientConnections` broker-level admin API (API key 93) that returns
information about all currently authenticated connections on a broker.

## What's Implemented

- Protocol schemas (request + response JSON)
- Request/Response wrapper classes
- API key registration (93)
- `SocketServer.collectClientConnections()` — enumerates channels across all processors
- `KafkaApis.handleListClientConnections()` — handler with principal/address/listener filters
- Authorization: requires `DESCRIBE` on `CLUSTER`

## What's NOT Implemented (deferred to full KIP PR)

- `AdminClient.listClientConnections()` method
- CLI tool (`kafka-client-connections.sh`)
- `ConnectedSince` field (schema includes it, returns 0)
- Unit/integration tests

## Test Results

### PLAINTEXT Mode (User:ANONYMOUS)

Broker started in KRaft combined mode with PLAINTEXT listener on port 9092.

```
$ bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep ListClient
    ListClientConnections(93): 0 [usable: 0]
```

With a console consumer connected:

```
=== ListClientConnections (KIP-1329 POC) ===
Error: NONE
Connections: 3

  [1] User:ANONYMOUS
      Address:  127.0.0.1:36084
      Listener: PLAINTEXT (PLAINTEXT)
      Software: unknown/unknown
      ConnID:   127.0.0.1:9092-127.0.0.1:36084-0-6

  [2] User:ANONYMOUS
      Address:  127.0.0.1:36058
      Listener: PLAINTEXT (PLAINTEXT)
      Software: apache-kafka-java/4.4.0-SNAPSHOT
      ConnID:   127.0.0.1:9092-127.0.0.1:36058-1-5

  [3] User:ANONYMOUS
      Address:  127.0.0.1:36068
      Listener: PLAINTEXT (PLAINTEXT)
      Software: apache-kafka-java/4.4.0-SNAPSHOT
      ConnID:   127.0.0.1:9092-127.0.0.1:36068-2-5
```

### Key Findings

1. **Thread safety is a non-issue**: `Selector.channels()` returns a snapshot (`new ArrayList<>(channels.values())`). Cross-thread access from request handler works without synchronization.

2. **Software info populated via KIP-511**: Clients that send `ApiVersionsRequest` with client software fields show `apache-kafka-java/4.4.0-SNAPSHOT`. Raw connections show `unknown/unknown`.

3. **`Selector` is `private[network]`**: Cannot access directly from `kafka.server` package. Solution: `SocketServer.collectClientConnections()` method encapsulates enumeration within the `kafka.network` package.

4. **`KafkaApis` needs `socketServer` reference**: Added as constructor parameter, passed from `BrokerServer`.

## How to Test

```bash
# Build
./gradlew releaseTarGz -x test

# Extract
tar xzf core/build/distributions/kafka_2.13-4.4.0-SNAPSHOT.tgz
cd kafka_2.13-4.4.0-SNAPSHOT

# Format and start (KRaft standalone)
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties
bin/kafka-server-start.sh -daemon config/server.properties

# Verify API is registered
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep ListClient

# Create a topic and connect a consumer
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --partitions 1 --replication-factor 1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --group test-group &

# Send ListClientConnections request (Python)
python3 ../test_list_connections.py
```

## Files Changed

| File | Change |
|------|--------|
| `clients/src/main/resources/common/message/ListClientConnectionsRequest.json` | NEW: Request schema |
| `clients/src/main/resources/common/message/ListClientConnectionsResponse.json` | NEW: Response schema |
| `clients/src/main/java/org/apache/kafka/common/requests/ListClientConnectionsRequest.java` | NEW: Request wrapper |
| `clients/src/main/java/org/apache/kafka/common/requests/ListClientConnectionsResponse.java` | NEW: Response wrapper |
| `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java` | Add enum entry (93) |
| `clients/src/main/java/org/apache/kafka/common/requests/AbstractRequest.java` | Add parse case |
| `clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java` | Add parse case |
| `core/src/main/scala/kafka/network/SocketServer.scala` | Add `collectClientConnections()` + `ConnectionInfo` |
| `core/src/main/scala/kafka/server/KafkaApis.scala` | Add `socketServer` param + handler |
| `core/src/main/scala/kafka/server/BrokerServer.scala` | Pass `socketServer` to `KafkaApis` |
| `core/src/main/java/kafka/server/builders/KafkaApisBuilder.java` | Pass null socketServer |
| `core/src/test/scala/unit/kafka/server/KafkaApisTest.scala` | Pass mock socketServer |
