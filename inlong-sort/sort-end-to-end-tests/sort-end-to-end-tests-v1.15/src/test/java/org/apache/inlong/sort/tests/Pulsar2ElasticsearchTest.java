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

import org.apache.http.HttpHost;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;

public class Pulsar2ElasticsearchTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Pulsar2ElasticsearchTest.class);

    public static final Logger PULSAR_LOG = LoggerFactory.getLogger(PulsarContainer.class);
    public static final Logger ELASTICSEARCH_LOG = LoggerFactory.getLogger(ElasticsearchContainer.class);

    private static final Path pulsarJar = TestUtils.getResource("sort-connector-pulsar.jar");
    private static final Path elasticsearchJar = TestUtils.getResource("sort-connector-elasticsearch-base.jar");

    private static final String sqlFile;

    static {
        try {
            URI pulsarSqlFile = Objects
                    .requireNonNull(Pulsar2ElasticsearchTest.class.getResource("/flinkSql/pulsar_test.sql")).toURI();
            sqlFile = Paths.get(pulsarSqlFile).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final PulsarContainer PULSAR = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.2"))
            .withNetwork(NETWORK)
            .withNetworkAliases("pulsar")
            .withLogConsumer(new Slf4jLogConsumer(PULSAR_LOG));

    @ClassRule
    public static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.9.3"))
                    .withExposedPorts(9200, 9300)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("elasticsearch")
                    .withLogConsumer(new Slf4jLogConsumer(ELASTICSEARCH_LOG));

    @Before
    public void setup() throws Exception {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializePulsarTopic();
        initializeElasticsearchIndex();
    }

    private void initializePulsarTopic() {
        try {
            Container.ExecResult result = PULSAR.execInContainer("bin/pulsar-admin", "topics", "create",
                    "persistent://public/default/test-topic");
            LOG.info("Create Pulsar topic: test-topic, std: {}", result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init Pulsar topic failed. Exit code:" + result.getExitCode());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeElasticsearchIndex() {
        // 使用 Elasticsearch 客户端创建索引
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", ELASTICSEARCH.getMappedPort(9200), "http")))) {
            client.indices().create(new CreateIndexRequest("test-index"), RequestOptions.DEFAULT);
            LOG.info("Created Elasticsearch index: test-index");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Elasticsearch index", e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (PULSAR != null) {
            PULSAR.stop();
        }

        if (ELASTICSEARCH != null) {
            ELASTICSEARCH.stop();
        }
    }

    @Test
    public void testPulsarToElasticsearch() throws Exception {
        submitSQLJob(sqlFile, pulsarJar, elasticsearchJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PULSAR.getPulsarBrokerUrl()).build()) {
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic("persistent://public/default/test-topic")
                    .create();
            producer.send("Test message 1");
            producer.send("Test message 2");
            producer.close();
        }

        // 查询 Elasticsearch 数据
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", ELASTICSEARCH.getMappedPort(9200), "http")))) {
            SearchRequest searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(QueryBuilders.matchAllQuery());
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Elasticsearch response: {}", searchResponse.getHits().getHits());
        } catch (IOException e) {
            LOG.error("Failed to query Elasticsearch", e);
        }
    }
}
