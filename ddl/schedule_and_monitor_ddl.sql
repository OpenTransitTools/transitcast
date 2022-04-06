create table if not exists data_set
(
    id                      bigserial                not null
        constraint data_set_pkey
            primary key,
    url                     text                     not null,
    e_tag                   text                     not null,
    last_modified_timestamp bigint                   not null,
    downloaded_at           timestamp with time zone not null,
    saved_at                timestamp with time zone,
    replaced_at             timestamp with time zone
);

create index data_set_idx1
    ON data_set
        (saved_at, replaced_at);

create table if not exists shape
(
    data_set_id         bigint           not null,
    shape_id            text             not null,
    shape_pt_lat        double precision not null,
    shape_pt_lon        double precision not null,
    shape_pt_sequence   int              not null,
    shape_dist_traveled double precision,
    constraint shape_pkey
        primary key (data_set_id, shape_id, shape_pt_sequence)
);

create table if not exists trip
(
    data_set_id     bigint not null,
    trip_id         text   not null,
    route_id        text,
    service_id      text,
    trip_headsign   text,
    trip_short_name text,
    block_id        text,
    shape_id        text,
    start_time      int,
    end_time        int,
    trip_distance   double precision,
    constraint trip_pkey
        primary key (data_set_id, trip_id)
);

create table if not exists stop_time
(
    data_set_id         bigint not null,
    trip_id             text   not null,
    stop_sequence       int    not null,
    stop_id             text,
    arrival_time        int,
    departure_time      int,
    shape_dist_traveled double precision,
    timepoint           int,
    constraint stop_time_pkey
        primary key (data_set_id, trip_id, stop_sequence)
);

create index stop_time_idx1
    ON stop_time
        (data_set_id, trip_id);

create table if not exists calendar
(
    data_set_id bigint not null,
    service_id  text   not null,
    monday      int    not null,
    tuesday     int    not null,
    wednesday   int    not null,
    thursday    int    not null,
    friday      int    not null,
    saturday    int    not null,
    sunday      int    not null,
    start_date  date   not null,
    end_date    date   not null,
    constraint calendar_pkey
        primary key (data_set_id, service_id)
);

create table calendar_date
(
    data_set_id    bigint not null,
    service_id     text   not null,
    date           date   not null,
    exception_type int    not null,
    constraint calendar_date_pkey
        primary key (data_set_id, service_id, date)
);

create table if not exists observed_stop_time
(
    observed_time         timestamp with time zone not null,
    stop_id               text                     not null,
    next_stop_id          text                     not null,
    vehicle_id            text                     not null,
    route_id              text                     not null,
    observed_at_stop      bool,
    observed_at_next_stop bool,
    stop_distance         double precision         not null,
    next_stop_distance    double precision         not null,
    travel_seconds        int                      not null,
    scheduled_seconds     int,
    scheduled_time        int,
    data_set_id           bigint                   not null,
    trip_id               text                     not null,
    created_at            timestamp with time zone,
    constraint observed_stop_time_pkey
        primary key (observed_time, stop_id, next_stop_id, vehicle_id)

) partition by range (observed_time);

create table if not exists trip_deviation
(
    id                  bigserial                not null,
    created_at          timestamp with time zone not null,
    trip_progress       double precision,
    data_set_id         bigint                   not null,
    trip_id             text                     not null,
    vehicle_id          text                     not null,
    at_stop             bool                     not null,
    delay               int                      not null,
    deviation_timestamp timestamp with time zone not null,
    constraint trip_deviation_pkey
        primary key (created_at, trip_id, vehicle_id)
) partition by range (created_at);