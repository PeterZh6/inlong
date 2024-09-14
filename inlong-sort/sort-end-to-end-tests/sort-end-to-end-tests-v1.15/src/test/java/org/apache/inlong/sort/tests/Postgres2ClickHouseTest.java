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

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end tests for sort-connector-postgres-cdc to ClickHouse.
 * Test flink sql PostgreSQL CDC to ClickHouse.
 */
public class Postgres2ClickHouseTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(Postgres2ClickHouseTest.class);

    private static final Path jdbcJar = TestUtils.getResource("sort-connector-jdbc.jar");
    private static final Path postgresCdcJar = TestUtils.getResource("sort-connector-postgres-cdc.jar");
    private static final Path postgresDriverJar = TestUtils.getResource("postgres-driver.jar");
    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths.get(Postgres2ClickHouseTest.class.getResource("/flinkSql/clickhouse_test.sql").toURI())
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final PostgreSQLContainer POSTGRES_CONTAINER = new PostgreSQLContainer<>(
            DockerImageName.parse("debezium/postgres:13"))
                    .withUsername("flinkuser")
                    .withPassword("flinkpw")
                    .withDatabaseName("test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final ClickHouseContainer CLICKHOUSE_CONTAINER =
            (ClickHouseContainer) new ClickHouseContainer("yandex/clickhouse-server:20.1.8.41")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("clickhouse")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        initializePostgresTable();
        initializeClickHouseTable();
    }

    @After
    public void teardown() {
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.stop();
        }
        if (CLICKHOUSE_CONTAINER != null) {
            CLICKHOUSE_CONTAINER.stop();
        }
    }

    private void initializeClickHouseTable() {
        try {
            Class.forName(CLICKHOUSE_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(CLICKHOUSE_CONTAINER.getJdbcUrl(), CLICKHOUSE_CONTAINER.getUsername(),
                            CLICKHOUSE_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute("create table test_output1 (\n"
                    + "       id Int32,\n"
                    + "       name Nullable(String),\n"
                    + "       description Nullable(String)\n"
                    + ")\n"
                    + "engine=MergeTree ORDER BY id;");
            stat.close();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializePostgresTable() {
        try (Connection conn =
                DriverManager.getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "CREATE TABLE test_input1 (\n"
                            + "  id SERIAL PRIMARY KEY,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512)\n"
                            + ");");
            stat.execute("ALTER TABLE test_input1 REPLICA IDENTITY FULL;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test flink sql postgresql cdc to ClickHouse
     *
     * @throws Exception The exception may throw when executing the case
     */
    @Test
    public void testClickHouseUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, jdbcJar, postgresCdcJar, postgresDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO test_input1 (id, name, description) "
                            + "VALUES (1,'jacket','water resistent white wind breaker');");
            stat.execute(
                    "INSERT INTO test_input1 (id, name, description) "
                            + "VALUES (2,'scooter','Big 2-wheel scooter ');");
            stat.execute(
                    "UPDATE test_input1 SET name = 'tom' WHERE id = 2;");
            stat.execute(
                    "DELETE FROM test_input1 WHERE id = 1;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        JdbcProxy proxy =
                new JdbcProxy(CLICKHOUSE_CONTAINER.getJdbcUrl(), CLICKHOUSE_CONTAINER.getUsername(),
                        CLICKHOUSE_CONTAINER.getPassword(),
                        CLICKHOUSE_CONTAINER.getDriverClassName());
        List<String> expectResult =
                Arrays.asList("2,tom,Big 2-wheel scooter ");

        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
    }
}
