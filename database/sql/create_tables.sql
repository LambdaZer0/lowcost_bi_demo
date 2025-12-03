create schema if not exists dm_lowcostBI;

DROP TABLE dm_lowcostbi.cause_mapping_table;
CREATE TABLE IF NOT EXISTS dm_lowcostbi.cause_mapping_table(
id serial primary key,
cause_key varchar(100),
cause_value varchar (1000),
cause VARCHAR(100)
);