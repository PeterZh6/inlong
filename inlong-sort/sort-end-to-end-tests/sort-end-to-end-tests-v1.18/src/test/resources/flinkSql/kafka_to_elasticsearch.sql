CREATE TABLE kafka_source (
    `message` STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);


CREATE TABLE file_sink (
    `message` STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/output_file',
    'format' = 'json'
);

INSERT INTO file_sink
SELECT * FROM kafka_source;
