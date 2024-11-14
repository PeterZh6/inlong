-- Create the source table to read data from Kafka
CREATE TABLE kafka_source (
    `message` STRING
) WITH (
    'connector' = 'kafka-inlong',  -- Use the Kafka connector
    'topic' = 'test-topic',         -- The Kafka topic to read from
    'bootstrap.servers' = 'localhost:9093',  -- Kafka broker address
    'group.id' = 'flink-group',     -- Consumer group ID
    'format' = 'json',              -- Data format (e.g., JSON)
    'scan.startup.mode' = 'earliest'  -- Start reading from the earliest message
);

-- Create the Elasticsearch sink to write data into Elasticsearch
CREATE TABLE elasticsearch_sink (
    `message` STRING
) WITH (
    'connector' = 'elasticsearch-7',  -- Elasticsearch connector
    'hosts' = 'http://localhost:9200',  -- Elasticsearch cluster address
    'index' = 'test-index',            -- Index name in Elasticsearch
    'document-type' = '_doc',          -- Document type (default '_doc' in ES 7)
    'format' = 'json'                  -- Data format to use for writing (JSON)
);

-- Insert data from Kafka source into Elasticsearch sink
INSERT INTO elasticsearch_sink
SELECT * FROM kafka_source;
