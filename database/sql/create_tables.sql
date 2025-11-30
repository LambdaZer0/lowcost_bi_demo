create schema if not exists dm_lowcostBI;
create table dm_lowcostbi.columns_mapping_table(
id serial primary key,
cause_key varchar(100),
cause_value varchar (1000)
);

select cause_key, cause_value from dm_lowcostbi.columns_mapping_table cmt ;
truncate table dm_lowcostbi.columns_mapping_table;