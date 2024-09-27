-- Pulsar Table
CREATE TABLE pulsar_source (
    `message` STRING
) WITH (
    'connector' = 'pulsar',
    'topic' = 'persistent://public/default/test-topic',
    'service-url' = 'pulsar://localhost:6650',
    'admin-url' = 'http://localhost:8080',
    'format' = 'json',
    'scan.startup.mode' = 'earliest'
);

-- ElasticSearch Table
CREATE TABLE elasticsearch_sink (
    `message` STRING
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://localhost:9200',
    'index' = 'test-index',
    'document-type' = '_doc',
    'format' = 'json'
);

-- Read data from Pulsar and write into ElasticSearch
INSERT INTO elasticsearch_sink
SELECT * FROM pulsar_source;
