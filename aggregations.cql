CREATE TABLE bbdata2.aggregations (
    minutes int,
    object_id int,
    date text,   -- form: MM-dd
    timestamp timestamp,

    last float,
    last_ts bigint,
    min float,
    max float,
    sum float,
    mean float,
    count int,
    -- std dev: keep intermediary data for late arrivals
    k float,  
    k_sum float,
    k_sum_2 float,
    std float,

    PRIMARY KEY ((minutes, object_id, date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);