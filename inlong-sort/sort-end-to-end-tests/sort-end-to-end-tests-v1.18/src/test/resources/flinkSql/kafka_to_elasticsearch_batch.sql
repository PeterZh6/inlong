CREATE TABLE kafka_source (
    `message` STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'scan.startup.timestamp-millis' = '0'
);


CREATE TABLE elasticsearch_sink (
    `message` STRING
) WITH (
    'connector' = 'elasticsearch7-inlong',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'test-index',
    'format' = 'json'
);


INSERT INTO elasticsearch_sink
SELECT `message`
FROM kafka_source
LIMIT 100;
