create table if not exists ml_model_type
(
    ml_model_type_id serial not null
        constraint ml_model_type_pk
            primary key,
    name             text   not null
);

create table if not exists ml_model
(
    ml_model_id                     bigserial not null
        constraint ml_model_pk
            primary key,
    version                         int       not null,
    start_timestamp                 timestamp with time zone,
    end_timestamp                   timestamp with time zone,
    ml_model_type_id                int       not null,
    model_name                      text      not null,
    train_flag                      boolean   not null,
    trained_timestamp               timestamp,
    avg_rmse                        double precision,
    ml_rmse                         double precision,
    feature_trained_start_timestamp timestamp with time zone,
    feature_trained_end_timestamp   timestamp with time zone,
    currently_relevant              bool not null default false,
    model_blob                      bytea,
    last_train_attempt_timestamp    timestamp with time zone,
    ml_model observed_stop_count    int,
    median                          double precision,
    average                         double precision,
    constraint ml_model_fk1
        foreign key (ml_model_type_id) references ml_model_type
);

create table if not exists ml_model_stop
(
    ml_model_stop_id bigserial not null
        constraint ml_model_stops_pk
            primary key,
    ml_model_id       bigint    not null,
    sequence          int       not null,
    stop_id           text      not null,
    next_stop_id      text      not null,
    constraint ml_model_stops_fk1
        foreign key (ml_model_id) references ml_model
);

insert into ml_model_type(name)
values ('Timepoints');
insert into ml_model_type(name)
values ('Stops');