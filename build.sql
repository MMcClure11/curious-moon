drop schema if exists import cascade;
create schema import;


drop table if exists events cascade;
drop table if exists teams cascade;
drop table if exists targets cascade;
drop table if exists spass_types cascade;
drop table if exists requests cascade;
drop table if exists event_types cascade;

create table import.master_plan(
  start_time_utc text,
  duration text,
  date text,
  team text,
  spass_type text,
  target text,
  request_name text,
  library_definition text,
  title text,
  description text
);

drop table if exists import.inms cascade;
CREATE TABLE import.inms(
	sclk text,
	uttime text,
	target text,
	time_ca text,
	targ_pos_x text,
	targ_pos_y text,
	targ_pos_z text,
	source text,
	data_reliability text,
	table_set_id text,
	coadd_cnt text,
	osp_fil_1_status text,
	oss_fil_2_status text,
	csp_fil_3_status text,
	css_fil_4_status text,
	seq_table text,
	cyc_num text,
	cyc_table text,
	scan_num text,
	trap_table text,
	sw_table text,
	mass_table text,
	focus_table text,
	da_table text,
	velocity_comp text,
	ipnum text,
	mass_per_charge text,
	os_lens2 text,
	os_lens1 text,
	os_lens4 text,
	os_lens3 text,
	qp_lens2 text,
	qp_lens1 text,
	qp_lens4 text,
	qp_lens3 text,
	qp_bias text,
	ion_defl2 text,
	ion_defl1 text,
	ion_defl4 text,
	ion_defl3 text,
	top_plate text,
	p_energy text,
	alt_t text,
	view_dir_t_x text,
	view_dir_t_y text,
	view_dir_t_z text,
	sc_pos_t_x text,
	sc_pos_t_y text,
	sc_pos_t_z text,
	sc_vel_t_x text,
	sc_vel_t_y text,
	sc_vel_t_z text,
	sc_vel_t_scx text,
	sc_vel_t_scy text,
	sc_vel_t_scz text,
	lst_t text,
	sza_t text,
	ss_long_t text,
	distance_s text,
	view_dir_s_x text,
	view_dir_s_y text,
	view_dir_s_z text,
	sc_pos_s_x text,
	sc_pos_s_y text,
	sc_pos_s_z text,
	sc_vel_s_x text,
	sc_vel_s_y text,
	sc_vel_s_z text,
	lst_s text,
	sza_s text,
	ss_long_s text,
	sc_att_angle_ra text,
	sc_att_angle_dec text,
	sc_att_angle_tw text,
	c1counts text,
	c2counts text
);

-- clean it up by removing headers
-- and empty rows
delete from import.inms
where sclk IS NULL or sclk = 'sclk';
COPY import.master_plan FROM '/Users/mmcclure/Code/curious_data/data/master_plan.csv' WITH DELIMITER ',' HEADER CSV;
COPY import.inms FROM '/Users/mmcclure/Code/curious_data/data/INMS/inms.csv' WITH DELIMITER ',' HEADER CSV;
-- TEAM
drop table if exists teams;
select distinct(team)
as description
into teams
from import.master_plan;

alter table teams
add id serial primary key;

-- SPASS TYPES
drop table if exists spass_types;
select distinct(spass_type)
as description
into spass_types
from import.master_plan;

alter table spass_types
add id serial primary key;

-- TARGET
drop table if exists targets;
select distinct(target)
as description
into targets
from import.master_plan;


alter table targets
add id serial primary key;

-- EVENT TYPES
drop table if exists event_types;
select distinct(library_definition)
as description
into event_types
from import.master_plan;


alter table event_types
add id serial primary key;

--REQUESTS
drop table if exists requests;
select distinct(request_name)
as description
into requests
from import.master_plan;

alter table requests
add id serial primary key;


create table events(
  id serial primary key,
  time_stamp timestamp not null,
  title varchar(500),
  description text,
  event_type_id int references event_types(id),
  target_id int references targets(id),
  team_id int references teams(id),
  request_id int references requests(id),
  spass_type_id int references spass_types(id)
);

insert into events(
  time_stamp, 
  title, 
  description, 
  event_type_id, 
  target_id, 
  team_id, 
  request_id,
	spass_type_id
)	
select 
  import.master_plan.start_time_utc::timestamp, 
  import.master_plan.title, 
  import.master_plan.description,
  event_types.id as event_type_id,
  targets.id as target_id,
  teams.id as team_id,
  requests.id as request_id,
  spass_types.id as spass_type_id
from import.master_plan
left join event_types 
  on event_types.description 
  = import.master_plan.library_definition
left join targets 
  on targets.description 
  = import.master_plan.target
left join teams 
  on teams.description 
  = import.master_plan.team
left join requests 
  on requests.description 
  = import.master_plan.request_name
left join spass_types 
  on spass_types.description 
  = import.master_plan.spass_type;
