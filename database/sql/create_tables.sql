create schema if not exists dm_lowcostBI;

create table if not exists dm_lowcostbi.cause_mapping_table(
id serial primary key,
cause_key varchar(100),
cause_value varchar (1000)
);
