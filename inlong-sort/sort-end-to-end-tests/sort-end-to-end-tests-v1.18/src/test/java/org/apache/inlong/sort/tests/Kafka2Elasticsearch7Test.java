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

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnvJRE8;
import org.apache.inlong.sort.tests.utils.PlaceholderResolver;
import org.apache.inlong.sort.tests.utils.TestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Kafka2Elasticsearch7Test extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2Elasticsearch7Test.class);
    public static final Logger KAFKA_LOG = LoggerFactory.getLogger(KafkaContainer.class);
    public static final Logger ELASTICSEARCH_LOGGER = LoggerFactory.getLogger(ElasticsearchContainer.class);

    private static final Path kafkaJar = TestUtils.getResource("sort-connector-kafka.jar");
    private static final Path elasticsearchJar = TestUtils.getResource("sort-connector-elasticsearch7.jar");

    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths
                    .get(Kafka2Elasticsearch7Test.class.getResource("/flinkSql/kafka_to_elasticsearch.sql").toURI())
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
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.24"))
                    .withExposedPorts(9200)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("elasticsearch")
                    .withLogConsumer(new Slf4jLogConsumer(ELASTICSEARCH_LOGGER));

    @Before
    public void setup() {
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
        URL url = Objects.requireNonNull(Kafka2Elasticsearch7Test.class.getResource("/env/" + fileName));

        try {
            Path file = Paths.get(url.toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(
                    new String(Files.readAllBytes(file), StandardCharsets.UTF_8),
                    properties);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeElasticsearchIndex() {
        try (RestClient restClient =
                RestClient.builder(new HttpHost("localhost", ELASTICSEARCH.getMappedPort(9200), "http")).build()) {
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // Create Elasticsearch index
            client.indices().create(c -> c.index("test-index"));
            LOG.info("Created Elasticsearch index: test-index");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Elasticsearch index", e);
        }
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
        submitSQLJob(sqlFile, kafkaJar, elasticsearchJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // Produce messages to Kafka
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
                new org.apache.kafka.clients.producer.KafkaProducer<>(getKafkaProducerConfig())) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("test-topic", "key1", "value1"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("test-topic", "key2", "value2"));
        }

        // Query Elasticsearch to verify data is ingested
        try (RestClient restClient =
                RestClient.builder(new HttpHost("localhost", ELASTICSEARCH.getMappedPort(9200), "http")).build()) {
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // Search Elasticsearch for the ingested data
            SearchRequest searchRequest =
                    new SearchRequest.Builder().index("test-index").query(q -> q.matchAll(m -> m)).build();
            SearchResponse<Object> searchResponse = client.search(searchRequest, Object.class);

            List<Hit<Object>> hits = searchResponse.hits().hits();
            LOG.info("Elasticsearch response: {}", hits);
        } catch (IOException e) {
            LOG.error("Failed to query Elasticsearch", e);
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
