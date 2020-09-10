CREATE TABLE trips
(
    `tripduration` Nullable(UInt64),
    `starttime` DateTime,
    `stoptime` Nullable(DateTime),
    `start station id` Nullable(UInt64),
    `start station name` Nullable(String),
    `start station latitude` Nullable(Float64),
    `start station longitude` Nullable(Float64),
    `end station id` Nullable(UInt64),
    `end station name` Nullable(String),
    `end station latitude` Nullable(Float64),
    `end station longitude` Nullable(Float64),
    `bikeid` Nullable(UInt64),
    `usertype` Nullable(String),
    `birth year` Nullable(UInt32),
    `gender` Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY toDate(starttime)