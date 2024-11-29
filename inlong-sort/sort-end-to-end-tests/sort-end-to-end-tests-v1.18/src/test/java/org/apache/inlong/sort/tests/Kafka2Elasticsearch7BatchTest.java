/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.tests;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnvJRE8;
import org.apache.inlong.sort.tests.utils.PlaceholderResolver;
import org.apache.inlong.sort.tests.utils.TestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Kafka2Elasticsearch7BatchTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2Elasticsearch7BatchTest.class);
    public static final Logger KAFKA_LOG = LoggerFactory.getLogger(KafkaContainer.class);
    public static final Logger ELASTICSEARCH_LOGGER = LoggerFactory.getLogger(ElasticsearchContainer.class);

    private static final Path kafkaJar = TestUtils.getResource("sort-connector-kafka.jar");
    private static final Path elasticsearchJar = TestUtils.getResource("sort-connector-elasticsearch7.jar");

    private static final int ELASTICSEARCH_DEFAULT_PORT = 9200;



    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths
                    .get(Kafka2Elasticsearch7BatchTest.class.getResource("/flinkSql/kafka_to_elasticsearch_batch.sql").toURI())
                    .toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withLogConsumer(new Slf4jLogConsumer(KAFKA_LOG));

    @ClassRule
    public static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.13"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("elasticsearch")
                    .withLogConsumer(new Slf4jLogConsumer(ELASTICSEARCH_LOGGER));

    @Before
    public void setup() throws IOException {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeKafkaTopic("test-topic");
        initializeElasticsearchIndex();
    }

    private void initializeKafkaTopic(String topic) {
        String fileName = "kafka_test_kafka_init.txt";
        int port = KafkaContainer.ZOOKEEPER_PORT;

        Map<String, Object> properties = new HashMap<>();
        properties.put("TOPIC", topic);
        properties.put("ZOOKEEPER_PORT", port);

        try {
            String createKafkaStatement = getCreateStatement(fileName, properties);
            ExecResult result = KAFKA.execInContainer("bash", "-c", createKafkaStatement);
            LOG.info("Create kafka topic: {}, std: {}", createKafkaStatement, result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init kafka topic failed. Exit code:" + result.getExitCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String getCreateStatement(String fileName, Map<String, Object> properties) {
        URL url = Objects.requireNonNull(Kafka2Elasticsearch7BatchTest.class.getResource("/env/" + fileName));

        try {
            Path file = Paths.get(url.toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(
                    new String(Files.readAllBytes(file), StandardCharsets.UTF_8),
                    properties);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeElasticsearchIndex() throws IOException {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", ELASTICSEARCH.getMappedPort(ELASTICSEARCH_DEFAULT_PORT), "http"))
                .build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);

        client.indices().create(c -> c.index("test-index"));
        LOG.info("Created Elasticsearch index: test-index");
    }

    @AfterClass
    public static void teardown() {
        if (KAFKA != null) {
            KAFKA.stop();
        }
        if (ELASTICSEARCH != null) {
            ELASTICSEARCH.stop();
        }
    }

    @Test
    public void testKafkaToElasticsearch() throws Exception {
        submitSQLBatchJob(sqlFile, kafkaJar, elasticsearchJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // Configure Kafka producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(getKafkaProducerConfig());

        // Global message ID to ensure unique message identifiers across all batches
        int globalMessageId = 1;

        // Number of messages per batch
        int batchSize = 5;

        // Number of batches to send
        int totalBatches = 3;

        // Produce batches of messages to Kafka
        for (int batch = 1; batch <= totalBatches; batch++) {
            // Send a batch of 5 messages
            for (int i = 0; i < batchSize; i++) {
                String message = String.format("{\"message\":\"Message %d from Kafka, Batch %d, ID %d\"}", i + 1, batch, globalMessageId++);
                producer.send(new ProducerRecord<>("test-topic", "key" + (batch * batchSize + i), message));
                LOG.info("Sent message {} from Batch {}", message, batch);
            }

            // wait for messages to be ingested into Elasticsearch
            Thread.sleep(2000);

            // After sending each batch, verify the batch has been ingested into Elasticsearch
            verifyBatchIngested(batchSize);

            // Wait for a short time to simulate time between batches
            Thread.sleep(2000);  // Sleep for 2 seconds between batches
        }
    }

    private void verifyBatchIngested(int batchSize) throws Exception {
        // Query Elasticsearch to verify that exactly `batchSize` messages have been ingested
        RestClient restClient = RestClient.builder(
                        new HttpHost("localhost", ELASTICSEARCH.getMappedPort(9200), "http"))
                .build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);

        List<String> messages = new ArrayList<>();
        int maxRetries = 10; // Maximum number of retries (10 seconds)
        int retryCount = 0;

        // Retry logic for verifying batch ingestion
        while (retryCount < maxRetries) {
            co.elastic.clients.elasticsearch.core.SearchRequest searchRequest =
                    new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                            .index("test-index")
                            .query(q -> q.matchAll(m -> m))
                            .build();

            co.elastic.clients.elasticsearch.core.SearchResponse<Map> response =
                    client.search(searchRequest, Map.class);

            // Extract `message` fields from the Elasticsearch response
            messages = response.hits().hits().stream()
                    .map(hit -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> source = hit.source();
                        if (source != null && source.containsKey("message")) {
                            return (String) source.get("message");
                        }
                        return null;
                    })
                    .filter(Objects::nonNull) // Remove null values
                    .collect(Collectors.toList());

            // If the number of messages matches the batch size, validate message content
            if (messages.size() == batchSize) {
                // Create the expected messages for this batch
                List<String> expectedMessages = new ArrayList<>();
                for (int i = 1; i <= batchSize; i++) {
                    expectedMessages.add(String.format("Message %d from Kafka, Batch %d", i, retryCount + 1));
                }

                if (new HashSet<>(messages).equals(new HashSet<>(expectedMessages))) {
                    LOG.info("Batch ingested successfully: {}", messages);
                    break;
                } else {
                    LOG.warn("Elasticsearch has messages, but they don't match the expected batch: {}", messages);
                }
            }

            // If not all messages are found or mismatch, retry
            if (retryCount == maxRetries - 1) {
                throw new AssertionError("Elasticsearch validation failed: Batch not ingested correctly.");
            }

            // Wait for 1 second before retrying
            Thread.sleep(1000);
            retryCount++;
        }
    }




    private java.util.Properties getKafkaProducerConfig() {
        java.util.Properties props = new java.util.Properties();
        String bootstrapServers = KAFKA.getBootstrapServers();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
