CREATE TABLE kafka_source (
    `message` STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'bootstrap.servers' = 'localhost:9093',
    'group.id' = 'flink-group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest'
);


CREATE TABLE elasticsearch_sink (
    `message` STRING
) WITH (
    'connector' = 'elasticsearch7-inlong',
    'hosts' = 'http://localhost:9200',
    'index' = 'test-index',
    'document-type' = '_doc',
    'format' = 'json'
);


INSERT INTO elasticsearch_sink
SELECT * FROM kafka_source;
