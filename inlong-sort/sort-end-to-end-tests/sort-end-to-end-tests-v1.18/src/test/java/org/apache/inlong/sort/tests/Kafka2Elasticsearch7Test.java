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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

public class Kafka2Elasticsearch7Test extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2Elasticsearch7Test.class);

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
                    .withNetworkAliases("kafka");

    @ClassRule
    public static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.24"))
                    .withExposedPorts(9200)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("elasticsearch");

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeKafkaTopic("test-topic");
        initializeElasticsearchIndex();
    }

    private void initializeKafkaTopic(String topic) {
        try {
            Container.ExecResult result = KAFKA.execInContainer("kafka-topics", "--create", "--topic", topic,
                    "--bootstrap-server", "localhost:9093", "--partitions", "1", "--replication-factor", "1");
            LOG.info("Kafka topic created: {}", result.getStdout());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Kafka topic", e);
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
        props.put("bootstrap.servers", "localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
