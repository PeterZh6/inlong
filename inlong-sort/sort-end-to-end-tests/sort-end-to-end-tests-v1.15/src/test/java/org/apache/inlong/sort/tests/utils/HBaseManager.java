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

package org.apache.inlong.sort.tests.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

public class HBaseManager {

    // ----------------------------------------------------------------------------------------
    // HBase Variables
    // ----------------------------------------------------------------------------------------
    public static final String INTER_CONTAINER_HBASE_ALIAS = "hbase";
    private static final String NEW_HBASE_REPOSITORY = "inlong-hbase";
    private static final String NEW_HBASE_TAG = "latest";
    private static final String HBASE_IMAGE_NAME = "harisekhon/hbase:2.4.11";
    private static final String DEFAULT_COLUMN_FAMILY = "cf1";
    public static final Logger HBASE_LOG = LoggerFactory.getLogger(HBaseContainer.class);

    static {
        GenericContainer oldHBase = new GenericContainer(HBASE_IMAGE_NAME);
        Startables.deepStart(Stream.of(oldHBase)).join();
        oldHBase.copyFileToContainer(MountableFile.forClasspathResource("/docker/hbase/start_hbase.sh"),
                "/data/hbase/");
        try {
            oldHBase.execInContainer("chmod", "+x", "/data/hbase/start_hbase.sh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        oldHBase.getDockerClient()
                .commitCmd(oldHBase.getContainerId())
                .withRepository(NEW_HBASE_REPOSITORY)
                .withTag(NEW_HBASE_TAG).exec();
        oldHBase.stop();
    }

    public static String getNewHBaseImageName() {
        return NEW_HBASE_REPOSITORY + ":" + NEW_HBASE_TAG;
    }

    public static void initializeHBaseTable(HBaseContainer HBASE_CONTAINER) {
        initializeHBaseTable(HBASE_CONTAINER, DEFAULT_COLUMN_FAMILY);
    }

    public static void initializeHBaseTable(HBaseContainer HBASE_CONTAINER, String columnFamily) {
        try (Connection conn = DriverManager.getConnection(HBASE_CONTAINER.getJdbcUrl(), HBASE_CONTAINER.getUsername(),
                HBASE_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE 'test_output1', '" + columnFamily + "'");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize HBase table", e);
        }
    }
}
