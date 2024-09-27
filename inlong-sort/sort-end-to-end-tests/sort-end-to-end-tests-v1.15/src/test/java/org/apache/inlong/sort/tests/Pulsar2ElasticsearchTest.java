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
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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

import static org.junit.Assert.assertTrue;

public class Pulsar2ElasticsearchTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Pulsar2ElasticsearchTest.class);

    public static final Logger PULSAR_LOG = LoggerFactory.getLogger(PulsarContainer.class);
    public static final Logger ELASTICSEARCH_LOG = LoggerFactory.getLogger(ElasticsearchContainer.class);

    private static final Path pulsarJar = TestUtils.getResource("sort-connector-pulsar.jar");
    private static final Path elasticsearchJar = TestUtils.getResource("sort-connector-elasticsearch.jar");

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
    public static final PulsarContainer PULSAR = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("pulsar")
            .withLogConsumer(new Slf4jLogConsumer(PULSAR_LOG));

    @ClassRule
    public static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.10.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("elasticsearch")
                    .withLogConsumer(new Slf4jLogConsumer(ELASTICSEARCH_LOG));

    @Before
    public void setup() {
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
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeElasticsearchIndex() {
        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build()) {
            Request request = new Request("PUT", "/test-index");
            Response response = restClient.performRequest(request);
            LOG.info("Create Elasticsearch index: {}, status: {}", "test-index",
                    response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            throw new RuntimeException(e);
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

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build()) {
            Request request = new Request("GET", "/test-index/_search");
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            LOG.info("Elasticsearch response: {}", responseBody);

            assertTrue(responseBody.contains("Test message 1"));
            assertTrue(responseBody.contains("Test message 2"));
        }
    }
}
