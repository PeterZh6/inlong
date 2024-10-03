CREATE TABLE test_input1
(
    `id` INT primary key,
    name STRING,
    description STRING
)
WITH ( 'connector' = 'postgres-cdc-inlong',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'test',
    'table-name' = 'test_input1',
    'schema-name' = 'public',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'inlong_slot',
    'debezium.slot.name' = 'inlong_slot');

CREATE TABLE test_output1
(
    `id` INT primary key,
    name STRING,
    description STRING
)
WITH ( 'connector' = 'hbase-2.2-inlong',
    'hbase.zookeeper.quorum' = 'hbase-zk',
    'hbase.zookeeper.znode.parent' = '/hbase',
    'table-name' = 'test_output1',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval-ms' = '1000',
    'sink.parallelism' = '2'
);

INSERT INTO test_output1
SELECT *
FROM test_input1;
