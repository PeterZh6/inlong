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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Docker container for HBase.
 */
@SuppressWarnings("rawtypes")
public class HBaseContainer extends GenericContainer<HBaseContainer> {

    public static final String IMAGE = "harisekhon/hbase";
    public static final Integer HBASE_MASTER_PORT = 16000;
    public static final Integer HBASE_THRIFT_PORT = 9090;
    public static final Integer HBASE_REST_PORT = 8080;

    private String databaseName = "test";
    private String username = "hbaseuser";
    private String password = "hbasepw";

    public HBaseContainer() {
        this(HBaseVersion.V2_4);
    }

    public HBaseContainer(HBaseVersion version) {
        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()).asCompatibleSubstituteFor("hbase"));
        addExposedPort(HBASE_MASTER_PORT);
        addExposedPort(HBASE_THRIFT_PORT);
        addExposedPort(HBASE_REST_PORT);
    }

    public HBaseContainer(String imageName) {
        super(DockerImageName.parse(imageName).asCompatibleSubstituteFor("hbase"));
        addExposedPort(HBASE_MASTER_PORT);
        addExposedPort(HBASE_THRIFT_PORT);
        addExposedPort(HBASE_REST_PORT);
    }

    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
            // throw new RuntimeException("HBase JDBC Driver not found!", e);
        }
    }

    public String getJdbcUrl(String databaseName) {
        return "jdbc:hbase://"
                + getHost()
                + ":"
                + getMappedPort(HBASE_THRIFT_PORT)
                + "/"
                + databaseName;
    }

    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(HBASE_THRIFT_PORT);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    protected String getTestQueryString() {
        return "status 'simple'";
    }

    /** HBase version enum. */
    public enum HBaseVersion {

        V2_2("2.2.7"),
        V2_4("2.4.11");

        private String version;

        HBaseVersion(String version) {
            this.version = version;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "HBaseVersion{" + "version='" + version + '\'' + '}';
        }
    }
}
