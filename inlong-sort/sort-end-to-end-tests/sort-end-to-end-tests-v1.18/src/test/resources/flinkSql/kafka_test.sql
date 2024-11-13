CREATE TABLE test_input (
    id INT,
    name STRING,
    description STRING
) WITH (
    'connector' = 'mysql-cdc-inlong',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'inlong',
    'database-name' = 'test',
    'table-name' = 'test_input',
    'scan.incremental.snapshot.enabled' = 'false',
    'jdbc.properties.useSSL' = 'false',
    'jdbc.properties.allowPublicKeyRetrieval' = 'true'
);

CREATE TABLE kafka_load (
    id INT,
    name STRING,
    description STRING
) WITH (
    'connector' = 'upsert-kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'csv',
    'value.format' = 'csv'
);

CREATE TABLE kafka_extract (
    id INT,
    name STRING,
    description STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'csv'
);

CREATE TABLE test_output (
    id INT,
    name STRING,
    description STRING
) WITH (
    'connector' = 'starrocks-inlong',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030',
    'load-url'='starrocks:8030',
    'database-name'='test',
    'table-name' = 'test_output1',
    'username' = 'inlong',
    'password' = 'inlong',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true',
    'sink.buffer-flush.interval-ms' = '1000'
);

-- Insert data from MySQL source into Kafka
INSERT INTO kafka_load SELECT * FROM test_input;

-- Insert data from Kafka into StarRocks sink
INSERT INTO test_output SELECT * FROM kafka_extract;
