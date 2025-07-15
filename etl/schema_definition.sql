-- DROP SCHEMA bikesharing;

CREATE SCHEMA bikesharing AUTHORIZATION ftm_app_bikesharing_upload;
-- bikesharing.bike_locations_temp definition

-- Drop table

-- DROP TABLE bikesharing.bike_locations_temp;

CREATE UNLOGGED TABLE bikesharing.bike_locations_temp (
	bike_id int4 NOT NULL,
	"time" timestamp NULL,
	station_id int4 NULL,
	geom public.geometry NULL,
	pedelec_battery int2 NULL,
	CONSTRAINT bike_locations_temp_pk PRIMARY KEY (bike_id)
)
WITH (
	autovacuum_enabled=false
);

-- Table Triggers

create trigger bike_loc_copy_trigger after update on
bikesharing.bike_locations_temp for each row execute function bikesharing.bike_loc_copy();


-- bikesharing.bike_locations_temp_pre definition

-- Drop table

-- DROP TABLE bikesharing.bike_locations_temp_pre;

CREATE UNLOGGED TABLE bikesharing.bike_locations_temp_pre (
	bike_id int4 NOT NULL,
	"time" timestamp NULL,
	station_id int4 NULL,
	geom public.geometry NULL,
	pedelec_battery int2 NULL,
	CONSTRAINT bike_locations_temp_pre_pk PRIMARY KEY (bike_id)
)
WITH (
	autovacuum_enabled=false
);


-- bikesharing.bike_types definition

-- Drop table

-- DROP TABLE bikesharing.bike_types;

CREATE TABLE bikesharing.bike_types (
	id int2 NOT NULL,
	vehicle_image text NULL,
	"name" text NULL,
	description text NULL,
	form_factor text NULL,
	rider_capacity int4 NULL,
	propulsion_type text NULL,
	max_range_meters float4 NULL,
	battery_capacity varchar NULL,
	CONSTRAINT bike_types_pkey PRIMARY KEY (id)
);


-- bikesharing.cities definition

-- Drop table

-- DROP TABLE bikesharing.cities;

CREATE TABLE bikesharing.cities (
	id int2 NOT NULL,
	country_id int2 NULL,
	lat float4 NULL,
	lng float4 NULL,
	alias text NULL,
	"name" text NULL,
	timezone text NULL,
	country text NULL,
	bounds public.geometry NULL,
	bike_types _int4 NULL,
	return_to_official_only bool NULL,
	website text NULL,
	"domain" text NULL,
	final_eval bool NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (id)
);


-- bikesharing.kpis_cache definition

-- Drop table

-- DROP TABLE bikesharing.kpis_cache;

CREATE TABLE bikesharing.kpis_cache (
	city_id int2 NULL,
	"timestamp" timestamptz NULL,
	n_trips numeric NULL,
	n_bikes numeric NULL,
	tdb numeric NULL,
	total_distance_km float8 NULL,
	avg_distance_km float8 NULL,
	kdb float8 NULL,
	total_duration interval NULL,
	avg_duration interval NULL,
	ddb interval NULL,
	tdc float8 NULL
);


-- bikesharing.scrapes definition

-- Drop table

-- DROP TABLE bikesharing.scrapes;

CREATE TABLE bikesharing.scrapes (
	time_scrape timestamptz NOT NULL,
	time_insert timestamptz NULL,
	archive text NULL,
	file text NULL,
	host text NULL,
	inserted_bikes int4 NULL,
	inserted_stations int4 NULL,
	discarded bool DEFAULT false NULL,
	bike_id_duplicates int4 NULL,
	station_id_duplicates int4 NULL,
	bike_city_check bool DEFAULT false NULL,
	CONSTRAINT scrapes_pk PRIMARY KEY (time_scrape)
);
CREATE INDEX scrapes_time_insert_idx ON bikesharing.scrapes USING btree (time_insert);
CREATE INDEX scrapes_time_scrape_discarded_idx ON bikesharing.scrapes USING btree (time_scrape, discarded);
CREATE INDEX scrapes_time_scrape_idx ON bikesharing.scrapes USING btree (time_scrape);


-- bikesharing.station_status_temp definition

-- Drop table

-- DROP TABLE bikesharing.station_status_temp;

CREATE UNLOGGED TABLE bikesharing.station_status_temp (
	station_id int4 NOT NULL,
	"time" timestamp NULL,
	bikes int2 NULL,
	booked_bikes int2 NULL,
	bikes_available_to_rent int2 NULL,
	free_racks int4 NULL,
	free_special_racks int2 NULL,
	maintenance bool NULL,
	CONSTRAINT station_status_temp_pk PRIMARY KEY (station_id)
)
WITH (
	autovacuum_enabled=false
);

-- Table Triggers

create trigger station_status_copy_trigger after update on
bikesharing.station_status_temp for each row execute function bikesharing.station_status_copy();


-- bikesharing.bikes definition

-- Drop table

-- DROP TABLE bikesharing.bikes;

CREATE TABLE bikesharing.bikes (
	id int4 NOT NULL,
	bike_type_id int2 NULL,
	city_id int2 NULL,
	first_seen timestamp NULL,
	last_seen timestamp NULL,
	boardcomputer int8 NULL,
	CONSTRAINT bikes_pkey PRIMARY KEY (id),
	CONSTRAINT bikes_bike_type_id_fkey FOREIGN KEY (bike_type_id) REFERENCES bikesharing.bike_types(id),
	CONSTRAINT bikes_city_id_fkey FOREIGN KEY (city_id) REFERENCES bikesharing.cities(id)
);

-- Table Triggers

create trigger bike_city_copy_prod_trigger after update on
bikesharing.bikes for each row execute function bikesharing.bike_city_prod_copy();
create trigger bike_city_copy_prod_trigger_ins after insert on
bikesharing.bikes for each row execute function bikesharing.bike_city_prod_copy_ins();


-- bikesharing.stations definition

-- Drop table

-- DROP TABLE bikesharing.stations;

CREATE TABLE bikesharing.stations (
	id int4 NOT NULL,
	city_id int2 NULL,
	"name" text NULL,
	"number" int4 NULL,
	first_seen timestamp NULL,
	last_seen timestamp NULL,
	terminal_type text NULL,
	place_type int2 NULL,
	bike_racks int4 NULL,
	special_racks int2 NULL,
	rack_locks bool NULL,
	address text NULL,
	geom public.geometry NULL,
	CONSTRAINT stations_pkey PRIMARY KEY (id),
	CONSTRAINT stations_city_id_fkey FOREIGN KEY (city_id) REFERENCES bikesharing.cities(id)
);


-- bikesharing.bike_city definition

-- Drop table

-- DROP TABLE bikesharing.bike_city;

CREATE TABLE bikesharing.bike_city (
	bike_id int4 NOT NULL,
	city_id int2 NOT NULL,
	time_start timestamp NOT NULL,
	time_end timestamp NULL,
	CONSTRAINT bike_city_check CHECK (((time_end > time_start) OR (time_end IS NULL))),
	CONSTRAINT bike_city_pk PRIMARY KEY (bike_id, city_id, time_start),
	CONSTRAINT bike_city_fk FOREIGN KEY (bike_id) REFERENCES bikesharing.bikes(id),
	CONSTRAINT bike_city_fk_1 FOREIGN KEY (city_id) REFERENCES bikesharing.cities(id)
);
CREATE INDEX bike_city_bike_id_idx ON bikesharing.bike_city USING btree (bike_id, time_start DESC);
CREATE INDEX bike_city_time_start_idx ON bikesharing.bike_city USING btree (time_start, time_end, bike_id);


-- bikesharing.bike_city_temp definition

-- Drop table

-- DROP TABLE bikesharing.bike_city_temp;

CREATE TABLE bikesharing.bike_city_temp (
	bike_id int4 NOT NULL,
	city_id int2 NOT NULL,
	"time" timestamp NULL,
	CONSTRAINT bike_city_temp_pk PRIMARY KEY (bike_id),
	CONSTRAINT bike_city_temp_fk FOREIGN KEY (bike_id) REFERENCES bikesharing.bikes(id),
	CONSTRAINT bike_city_temp_fk_1 FOREIGN KEY (city_id) REFERENCES bikesharing.cities(id)
);

-- Table Triggers

create trigger bike_city_copy_trigger after update on
bikesharing.bike_city_temp for each row execute function bikesharing.bike_city_copy();
create trigger bike_city_copy_trigger_ins after insert on
bikesharing.bike_city_temp for each row execute function bikesharing.bike_city_copy_ins();


-- bikesharing.bike_locations definition

-- Drop table

-- DROP TABLE bikesharing.bike_locations;

CREATE TABLE bikesharing.bike_locations (
	bike_id int4 NOT NULL,
	"time" timestamp NOT NULL,
	station_id int4 NULL,
	geom public.geometry NULL,
	pedelec_battery int2 NULL,
	CONSTRAINT bike_locations_pk PRIMARY KEY (bike_id, "time"),
	CONSTRAINT bike_locations__part_bike_id_fkey FOREIGN KEY (bike_id) REFERENCES bikesharing.bikes(id),
	CONSTRAINT bike_locations_part_station_id_fkey FOREIGN KEY (station_id) REFERENCES bikesharing.stations(id)
)
PARTITION BY RANGE ("time");
CREATE INDEX bike_locations_bike_id_time_desc_idx ON ONLY bikesharing.bike_locations USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_discarded definition

-- Drop table

-- DROP TABLE bikesharing.bike_locations_discarded;

CREATE TABLE bikesharing.bike_locations_discarded (
	bike_id int4 NOT NULL,
	"time" timestamp NOT NULL,
	station_id int4 NULL,
	geom public.geometry NULL,
	pedelec_battery int2 NULL,
	CONSTRAINT bike_locations_chaos_pk PRIMARY KEY (bike_id, "time"),
	CONSTRAINT bike_locations_bike_id_fkey FOREIGN KEY (bike_id) REFERENCES bikesharing.bikes(id),
	CONSTRAINT bike_locations_station_id_fkey FOREIGN KEY (station_id) REFERENCES bikesharing.stations(id)
);


-- bikesharing.bike_locations_part_2022_01 definition

CREATE TABLE bikesharing.bike_locations_part_2022_01 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2022-02-01 00:00:00');
CREATE INDEX bike_locations_part_2022_01_bike_id_time_idx ON bikesharing.bike_locations_part_2022_01 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_02 definition

CREATE TABLE bikesharing.bike_locations_part_2022_02 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-02-01 00:00:00') TO ('2022-03-01 00:00:00');
CREATE INDEX bike_locations_part_2022_02_bike_id_time_idx ON bikesharing.bike_locations_part_2022_02 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_03 definition

CREATE TABLE bikesharing.bike_locations_part_2022_03 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-03-01 00:00:00') TO ('2022-04-01 00:00:00');
CREATE INDEX bike_locations_part_2022_03_bike_id_time_idx ON bikesharing.bike_locations_part_2022_03 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_04 definition

CREATE TABLE bikesharing.bike_locations_part_2022_04 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-04-01 00:00:00') TO ('2022-05-01 00:00:00');
CREATE INDEX bike_locations_part_2022_04_bike_id_time_idx ON bikesharing.bike_locations_part_2022_04 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_05 definition

CREATE TABLE bikesharing.bike_locations_part_2022_05 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-05-01 00:00:00') TO ('2022-06-01 00:00:00');
CREATE INDEX bike_locations_part_2022_05_bike_id_time_idx ON bikesharing.bike_locations_part_2022_05 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_06 definition

CREATE TABLE bikesharing.bike_locations_part_2022_06 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-06-01 00:00:00') TO ('2022-07-01 00:00:00');
CREATE INDEX bike_locations_part_2022_06_bike_id_time_idx ON bikesharing.bike_locations_part_2022_06 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_07 definition

CREATE TABLE bikesharing.bike_locations_part_2022_07 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-07-01 00:00:00') TO ('2022-08-01 00:00:00');
CREATE INDEX bike_locations_part_2022_07_bike_id_time_idx ON bikesharing.bike_locations_part_2022_07 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_08 definition

CREATE TABLE bikesharing.bike_locations_part_2022_08 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-08-01 00:00:00') TO ('2022-09-01 00:00:00');
CREATE INDEX bike_locations_part_2022_08_bike_id_time_idx ON bikesharing.bike_locations_part_2022_08 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_09 definition

CREATE TABLE bikesharing.bike_locations_part_2022_09 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-09-01 00:00:00') TO ('2022-10-01 00:00:00');
CREATE INDEX bike_locations_part_2022_09_bike_id_time_idx ON bikesharing.bike_locations_part_2022_09 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_10 definition

CREATE TABLE bikesharing.bike_locations_part_2022_10 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-10-01 00:00:00') TO ('2022-11-01 00:00:00');
CREATE INDEX bike_locations_part_2022_10_bike_id_time_idx ON bikesharing.bike_locations_part_2022_10 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_11 definition

CREATE TABLE bikesharing.bike_locations_part_2022_11 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-11-01 00:00:00') TO ('2022-12-01 00:00:00');
CREATE INDEX bike_locations_part_2022_11_bike_id_time_idx ON bikesharing.bike_locations_part_2022_11 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2022_12 definition

CREATE TABLE bikesharing.bike_locations_part_2022_12 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2022-12-01 00:00:00') TO ('2023-01-01 00:00:00');
CREATE INDEX bike_locations_part_2022_12_bike_id_time_idx ON bikesharing.bike_locations_part_2022_12 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_01 definition

CREATE TABLE bikesharing.bike_locations_part_2023_01 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-02-01 00:00:00');
CREATE INDEX bike_locations_part_2023_01_bike_id_time_idx ON bikesharing.bike_locations_part_2023_01 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_02 definition

CREATE TABLE bikesharing.bike_locations_part_2023_02 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-02-01 00:00:00') TO ('2023-03-01 00:00:00');
CREATE INDEX bike_locations_part_2023_02_bike_id_time_idx ON bikesharing.bike_locations_part_2023_02 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_03 definition

CREATE TABLE bikesharing.bike_locations_part_2023_03 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-03-01 00:00:00') TO ('2023-04-01 00:00:00');
CREATE INDEX bike_locations_part_2023_03_bike_id_time_idx ON bikesharing.bike_locations_part_2023_03 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_04 definition

CREATE TABLE bikesharing.bike_locations_part_2023_04 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-04-01 00:00:00') TO ('2023-05-01 00:00:00');
CREATE INDEX bike_locations_part_2023_04_bike_id_time_idx ON bikesharing.bike_locations_part_2023_04 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_05 definition

CREATE TABLE bikesharing.bike_locations_part_2023_05 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-05-01 00:00:00') TO ('2023-06-01 00:00:00');
CREATE INDEX bike_locations_part_2023_05_bike_id_time_idx ON bikesharing.bike_locations_part_2023_05 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_06 definition

CREATE TABLE bikesharing.bike_locations_part_2023_06 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-06-01 00:00:00') TO ('2023-07-01 00:00:00');
CREATE INDEX bike_locations_part_2023_06_bike_id_time_idx ON bikesharing.bike_locations_part_2023_06 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_07 definition

CREATE TABLE bikesharing.bike_locations_part_2023_07 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-07-01 00:00:00') TO ('2023-08-01 00:00:00');
CREATE INDEX bike_locations_part_2023_07_bike_id_time_idx ON bikesharing.bike_locations_part_2023_07 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_08 definition

CREATE TABLE bikesharing.bike_locations_part_2023_08 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-08-01 00:00:00') TO ('2023-09-01 00:00:00');
CREATE INDEX bike_locations_part_2023_08_bike_id_time_idx ON bikesharing.bike_locations_part_2023_08 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_09 definition

CREATE TABLE bikesharing.bike_locations_part_2023_09 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-09-01 00:00:00') TO ('2023-10-01 00:00:00');
CREATE INDEX bike_locations_part_2023_09_bike_id_time_idx ON bikesharing.bike_locations_part_2023_09 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_10 definition

CREATE TABLE bikesharing.bike_locations_part_2023_10 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-11-01 00:00:00');
CREATE INDEX bike_locations_part_2023_10_bike_id_time_idx ON bikesharing.bike_locations_part_2023_10 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_11 definition

CREATE TABLE bikesharing.bike_locations_part_2023_11 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-11-01 00:00:00') TO ('2023-12-01 00:00:00');
CREATE INDEX bike_locations_part_2023_11_bike_id_time_idx ON bikesharing.bike_locations_part_2023_11 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2023_12 definition

CREATE TABLE bikesharing.bike_locations_part_2023_12 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2023-12-01 00:00:00') TO ('2024-01-01 00:00:00');
CREATE INDEX bike_locations_part_2023_12_bike_id_time_idx ON bikesharing.bike_locations_part_2023_12 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_01 definition

CREATE TABLE bikesharing.bike_locations_part_2024_01 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00');
CREATE INDEX bike_locations_part_2024_01_bike_id_time_idx ON bikesharing.bike_locations_part_2024_01 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_02 definition

CREATE TABLE bikesharing.bike_locations_part_2024_02 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-02-01 00:00:00') TO ('2024-03-01 00:00:00');
CREATE INDEX bike_locations_part_2024_02_bike_id_time_idx ON bikesharing.bike_locations_part_2024_02 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_03 definition

CREATE TABLE bikesharing.bike_locations_part_2024_03 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-03-01 00:00:00') TO ('2024-04-01 00:00:00');
CREATE INDEX bike_locations_part_2024_03_bike_id_time_idx ON bikesharing.bike_locations_part_2024_03 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_04 definition

CREATE TABLE bikesharing.bike_locations_part_2024_04 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-04-01 00:00:00') TO ('2024-05-01 00:00:00');
CREATE INDEX bike_locations_part_2024_04_bike_id_time_idx ON bikesharing.bike_locations_part_2024_04 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_05 definition

CREATE TABLE bikesharing.bike_locations_part_2024_05 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-05-01 00:00:00') TO ('2024-06-01 00:00:00');
CREATE INDEX bike_locations_part_2024_05_bike_id_time_idx ON bikesharing.bike_locations_part_2024_05 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_06 definition

CREATE TABLE bikesharing.bike_locations_part_2024_06 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-06-01 00:00:00') TO ('2024-07-01 00:00:00');
CREATE INDEX bike_locations_part_2024_06_bike_id_time_idx ON bikesharing.bike_locations_part_2024_06 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_07 definition

CREATE TABLE bikesharing.bike_locations_part_2024_07 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-07-01 00:00:00') TO ('2024-08-01 00:00:00');
CREATE INDEX bike_locations_part_2024_07_bike_id_time_idx ON bikesharing.bike_locations_part_2024_07 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_08 definition

CREATE TABLE bikesharing.bike_locations_part_2024_08 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-08-01 00:00:00') TO ('2024-09-01 00:00:00');
CREATE INDEX bike_locations_part_2024_08_bike_id_time_idx ON bikesharing.bike_locations_part_2024_08 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_09 definition

CREATE TABLE bikesharing.bike_locations_part_2024_09 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-09-01 00:00:00') TO ('2024-10-01 00:00:00');
CREATE INDEX bike_locations_part_2024_09_bike_id_time_idx ON bikesharing.bike_locations_part_2024_09 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_10 definition

CREATE TABLE bikesharing.bike_locations_part_2024_10 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-10-01 00:00:00') TO ('2024-11-01 00:00:00');
CREATE INDEX bike_locations_part_2024_10_bike_id_time_idx ON bikesharing.bike_locations_part_2024_10 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_11 definition

CREATE TABLE bikesharing.bike_locations_part_2024_11 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-11-01 00:00:00') TO ('2024-12-01 00:00:00');
CREATE INDEX bike_locations_part_2024_11_bike_id_time_idx ON bikesharing.bike_locations_part_2024_11 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2024_12 definition

CREATE TABLE bikesharing.bike_locations_part_2024_12 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2024-12-01 00:00:00') TO ('2025-01-01 00:00:00');
CREATE INDEX bike_locations_part_2024_12_bike_id_time_idx ON bikesharing.bike_locations_part_2024_12 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_01 definition

CREATE TABLE bikesharing.bike_locations_part_2025_01 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-02-01 00:00:00');
CREATE INDEX bike_locations_part_2025_01_bike_id_time_idx ON bikesharing.bike_locations_part_2025_01 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_02 definition

CREATE TABLE bikesharing.bike_locations_part_2025_02 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-02-01 00:00:00') TO ('2025-03-01 00:00:00');
CREATE INDEX bike_locations_part_2025_02_bike_id_time_idx ON bikesharing.bike_locations_part_2025_02 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_03 definition

CREATE TABLE bikesharing.bike_locations_part_2025_03 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-03-01 00:00:00') TO ('2025-04-01 00:00:00');
CREATE INDEX bike_locations_part_2025_03_bike_id_time_idx ON bikesharing.bike_locations_part_2025_03 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_04 definition

CREATE TABLE bikesharing.bike_locations_part_2025_04 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-04-01 00:00:00') TO ('2025-05-01 00:00:00');
CREATE INDEX bike_locations_part_2025_04_bike_id_time_idx ON bikesharing.bike_locations_part_2025_04 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_05 definition

CREATE TABLE bikesharing.bike_locations_part_2025_05 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-05-01 00:00:00') TO ('2025-06-01 00:00:00');
CREATE INDEX bike_locations_part_2025_05_bike_id_time_idx ON bikesharing.bike_locations_part_2025_05 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_06 definition

CREATE TABLE bikesharing.bike_locations_part_2025_06 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-06-01 00:00:00') TO ('2025-07-01 00:00:00');
CREATE INDEX bike_locations_part_2025_06_bike_id_time_idx ON bikesharing.bike_locations_part_2025_06 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_07 definition

CREATE TABLE bikesharing.bike_locations_part_2025_07 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-07-01 00:00:00') TO ('2025-08-01 00:00:00');
CREATE INDEX bike_locations_part_2025_07_bike_id_time_idx ON bikesharing.bike_locations_part_2025_07 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_08 definition

CREATE TABLE bikesharing.bike_locations_part_2025_08 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-08-01 00:00:00') TO ('2025-09-01 00:00:00');
CREATE INDEX bike_locations_part_2025_08_bike_id_time_idx ON bikesharing.bike_locations_part_2025_08 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_09 definition

CREATE TABLE bikesharing.bike_locations_part_2025_09 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-09-01 00:00:00') TO ('2025-10-01 00:00:00');
CREATE INDEX bike_locations_part_2025_09_bike_id_time_idx ON bikesharing.bike_locations_part_2025_09 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_10 definition

CREATE TABLE bikesharing.bike_locations_part_2025_10 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-10-01 00:00:00') TO ('2025-11-01 00:00:00');
CREATE INDEX bike_locations_part_2025_10_bike_id_time_idx ON bikesharing.bike_locations_part_2025_10 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_11 definition

CREATE TABLE bikesharing.bike_locations_part_2025_11 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-11-01 00:00:00') TO ('2025-12-01 00:00:00');
CREATE INDEX bike_locations_part_2025_11_bike_id_time_idx ON bikesharing.bike_locations_part_2025_11 USING btree (bike_id, "time" DESC);


-- bikesharing.bike_locations_part_2025_12 definition

CREATE TABLE bikesharing.bike_locations_part_2025_12 PARTITION OF bikesharing.bike_locations  FOR VALUES FROM ('2025-12-01 00:00:00') TO ('2026-01-01 00:00:00');
CREATE INDEX bike_locations_part_2025_12_bike_id_time_idx ON bikesharing.bike_locations_part_2025_12 USING btree (bike_id, "time" DESC);


-- bikesharing.station_status definition

-- Drop table

-- DROP TABLE bikesharing.station_status;

CREATE TABLE bikesharing.station_status (
	station_id int4 NOT NULL,
	"time" timestamp NOT NULL,
	bikes int2 NULL,
	booked_bikes int2 NULL,
	bikes_available_to_rent int2 NULL,
	free_racks int4 NULL,
	free_special_racks int2 NULL,
	maintenance bool NULL,
	CONSTRAINT station_status_pk PRIMARY KEY (station_id, "time")
)
PARTITION BY RANGE ("time");


-- bikesharing.station_status_discarded definition

-- Drop table

-- DROP TABLE bikesharing.station_status_discarded;

CREATE TABLE bikesharing.station_status_discarded (
	station_id int4 NOT NULL,
	"time" timestamp NOT NULL,
	bikes int2 NULL,
	booked_bikes int2 NULL,
	bikes_available_to_rent int2 NULL,
	free_racks int2 NULL,
	free_special_racks int2 NULL,
	maintenance bool NULL,
	CONSTRAINT station_status_chaos_pk PRIMARY KEY (station_id, "time")
);


-- bikesharing.station_status_part_2022_01 definition

CREATE TABLE bikesharing.station_status_part_2022_01 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2022-02-01 00:00:00');


-- bikesharing.station_status_part_2022_02 definition

CREATE TABLE bikesharing.station_status_part_2022_02 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-02-01 00:00:00') TO ('2022-03-01 00:00:00');


-- bikesharing.station_status_part_2022_03 definition

CREATE TABLE bikesharing.station_status_part_2022_03 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-03-01 00:00:00') TO ('2022-04-01 00:00:00');


-- bikesharing.station_status_part_2022_04 definition

CREATE TABLE bikesharing.station_status_part_2022_04 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-04-01 00:00:00') TO ('2022-05-01 00:00:00');


-- bikesharing.station_status_part_2022_05 definition

CREATE TABLE bikesharing.station_status_part_2022_05 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-05-01 00:00:00') TO ('2022-06-01 00:00:00');


-- bikesharing.station_status_part_2022_06 definition

CREATE TABLE bikesharing.station_status_part_2022_06 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-06-01 00:00:00') TO ('2022-07-01 00:00:00');


-- bikesharing.station_status_part_2022_07 definition

CREATE TABLE bikesharing.station_status_part_2022_07 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-07-01 00:00:00') TO ('2022-08-01 00:00:00');


-- bikesharing.station_status_part_2022_08 definition

CREATE TABLE bikesharing.station_status_part_2022_08 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-08-01 00:00:00') TO ('2022-09-01 00:00:00');


-- bikesharing.station_status_part_2022_09 definition

CREATE TABLE bikesharing.station_status_part_2022_09 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-09-01 00:00:00') TO ('2022-10-01 00:00:00');


-- bikesharing.station_status_part_2022_10 definition

CREATE TABLE bikesharing.station_status_part_2022_10 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-10-01 00:00:00') TO ('2022-11-01 00:00:00');


-- bikesharing.station_status_part_2022_11 definition

CREATE TABLE bikesharing.station_status_part_2022_11 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-11-01 00:00:00') TO ('2022-12-01 00:00:00');


-- bikesharing.station_status_part_2022_12 definition

CREATE TABLE bikesharing.station_status_part_2022_12 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2022-12-01 00:00:00') TO ('2023-01-01 00:00:00');


-- bikesharing.station_status_part_2023_01 definition

CREATE TABLE bikesharing.station_status_part_2023_01 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-02-01 00:00:00');


-- bikesharing.station_status_part_2023_02 definition

CREATE TABLE bikesharing.station_status_part_2023_02 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-02-01 00:00:00') TO ('2023-03-01 00:00:00');


-- bikesharing.station_status_part_2023_03 definition

CREATE TABLE bikesharing.station_status_part_2023_03 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-03-01 00:00:00') TO ('2023-04-01 00:00:00');


-- bikesharing.station_status_part_2023_04 definition

CREATE TABLE bikesharing.station_status_part_2023_04 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-04-01 00:00:00') TO ('2023-05-01 00:00:00');


-- bikesharing.station_status_part_2023_05 definition

CREATE TABLE bikesharing.station_status_part_2023_05 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-05-01 00:00:00') TO ('2023-06-01 00:00:00');


-- bikesharing.station_status_part_2023_06 definition

CREATE TABLE bikesharing.station_status_part_2023_06 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-06-01 00:00:00') TO ('2023-07-01 00:00:00');


-- bikesharing.station_status_part_2023_07 definition

CREATE TABLE bikesharing.station_status_part_2023_07 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-07-01 00:00:00') TO ('2023-08-01 00:00:00');


-- bikesharing.station_status_part_2023_08 definition

CREATE TABLE bikesharing.station_status_part_2023_08 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-08-01 00:00:00') TO ('2023-09-01 00:00:00');


-- bikesharing.station_status_part_2023_09 definition

CREATE TABLE bikesharing.station_status_part_2023_09 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-09-01 00:00:00') TO ('2023-10-01 00:00:00');


-- bikesharing.station_status_part_2023_10 definition

CREATE TABLE bikesharing.station_status_part_2023_10 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-11-01 00:00:00');


-- bikesharing.station_status_part_2023_11 definition

CREATE TABLE bikesharing.station_status_part_2023_11 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-11-01 00:00:00') TO ('2023-12-01 00:00:00');


-- bikesharing.station_status_part_2023_12 definition

CREATE TABLE bikesharing.station_status_part_2023_12 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2023-12-01 00:00:00') TO ('2024-01-01 00:00:00');


-- bikesharing.station_status_part_2024_01 definition

CREATE TABLE bikesharing.station_status_part_2024_01 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00');


-- bikesharing.station_status_part_2024_02 definition

CREATE TABLE bikesharing.station_status_part_2024_02 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-02-01 00:00:00') TO ('2024-03-01 00:00:00');


-- bikesharing.station_status_part_2024_03 definition

CREATE TABLE bikesharing.station_status_part_2024_03 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-03-01 00:00:00') TO ('2024-04-01 00:00:00');


-- bikesharing.station_status_part_2024_04 definition

CREATE TABLE bikesharing.station_status_part_2024_04 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-04-01 00:00:00') TO ('2024-05-01 00:00:00');


-- bikesharing.station_status_part_2024_05 definition

CREATE TABLE bikesharing.station_status_part_2024_05 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-05-01 00:00:00') TO ('2024-06-01 00:00:00');


-- bikesharing.station_status_part_2024_06 definition

CREATE TABLE bikesharing.station_status_part_2024_06 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-06-01 00:00:00') TO ('2024-07-01 00:00:00');


-- bikesharing.station_status_part_2024_07 definition

CREATE TABLE bikesharing.station_status_part_2024_07 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-07-01 00:00:00') TO ('2024-08-01 00:00:00');


-- bikesharing.station_status_part_2024_08 definition

CREATE TABLE bikesharing.station_status_part_2024_08 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-08-01 00:00:00') TO ('2024-09-01 00:00:00');


-- bikesharing.station_status_part_2024_09 definition

CREATE TABLE bikesharing.station_status_part_2024_09 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-09-01 00:00:00') TO ('2024-10-01 00:00:00');


-- bikesharing.station_status_part_2024_10 definition

CREATE TABLE bikesharing.station_status_part_2024_10 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-10-01 00:00:00') TO ('2024-11-01 00:00:00');


-- bikesharing.station_status_part_2024_11 definition

CREATE TABLE bikesharing.station_status_part_2024_11 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-11-01 00:00:00') TO ('2024-12-01 00:00:00');


-- bikesharing.station_status_part_2024_12 definition

CREATE TABLE bikesharing.station_status_part_2024_12 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2024-12-01 00:00:00') TO ('2025-01-01 00:00:00');


-- bikesharing.station_status_part_2025_01 definition

CREATE TABLE bikesharing.station_status_part_2025_01 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-02-01 00:00:00');


-- bikesharing.station_status_part_2025_02 definition

CREATE TABLE bikesharing.station_status_part_2025_02 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-02-01 00:00:00') TO ('2025-03-01 00:00:00');


-- bikesharing.station_status_part_2025_03 definition

CREATE TABLE bikesharing.station_status_part_2025_03 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-03-01 00:00:00') TO ('2025-04-01 00:00:00');


-- bikesharing.station_status_part_2025_04 definition

CREATE TABLE bikesharing.station_status_part_2025_04 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-04-01 00:00:00') TO ('2025-05-01 00:00:00');


-- bikesharing.station_status_part_2025_05 definition

CREATE TABLE bikesharing.station_status_part_2025_05 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-05-01 00:00:00') TO ('2025-06-01 00:00:00');


-- bikesharing.station_status_part_2025_06 definition

CREATE TABLE bikesharing.station_status_part_2025_06 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-06-01 00:00:00') TO ('2025-07-01 00:00:00');


-- bikesharing.station_status_part_2025_07 definition

CREATE TABLE bikesharing.station_status_part_2025_07 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-07-01 00:00:00') TO ('2025-08-01 00:00:00');


-- bikesharing.station_status_part_2025_08 definition

CREATE TABLE bikesharing.station_status_part_2025_08 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-08-01 00:00:00') TO ('2025-09-01 00:00:00');


-- bikesharing.station_status_part_2025_09 definition

CREATE TABLE bikesharing.station_status_part_2025_09 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-09-01 00:00:00') TO ('2025-10-01 00:00:00');


-- bikesharing.station_status_part_2025_10 definition

CREATE TABLE bikesharing.station_status_part_2025_10 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-10-01 00:00:00') TO ('2025-11-01 00:00:00');


-- bikesharing.station_status_part_2025_11 definition

CREATE TABLE bikesharing.station_status_part_2025_11 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-11-01 00:00:00') TO ('2025-12-01 00:00:00');


-- bikesharing.station_status_part_2025_12 definition

CREATE TABLE bikesharing.station_status_part_2025_12 PARTITION OF bikesharing.station_status  FOR VALUES FROM ('2025-12-01 00:00:00') TO ('2026-01-01 00:00:00');


-- bikesharing.station_status_part_default definition

CREATE TABLE bikesharing.station_status_part_default PARTITION OF bikesharing.station_status  DEFAULT;


-- bikesharing.city_eurostat_region definition

-- Drop table

-- DROP TABLE bikesharing.city_eurostat_region;

CREATE TABLE bikesharing.city_eurostat_region (
	city_id int2 NOT NULL,
	region text NULL,
	CONSTRAINT city_eurostat_region_pk PRIMARY KEY (city_id)
);


-- bikesharing.city_lau definition

-- Drop table

-- DROP TABLE bikesharing.city_lau;

CREATE TABLE bikesharing.city_lau (
	city_id int2 NOT NULL,
	cntr_lau_code text NULL,
	CONSTRAINT city_lau_pk PRIMARY KEY (city_id)
);


-- bikesharing.bike_city_usage source

CREATE MATERIALIZED VIEW bikesharing.bike_city_usage
TABLESPACE pg_default
AS SELECT bc.city_id,
    bc.bike_id,
    (bc.time_start AT TIME ZONE 'utc'::text) AS period_start,
    COALESCE((bc.time_end AT TIME ZONE 'utc'::text), ('2023-07-16 00:00:00'::timestamp without time zone AT TIME ZONE 'utc'::text)) AS period_end,
    COALESCE((bc.time_end AT TIME ZONE 'utc'::text), ('2023-07-16 00:00:00'::timestamp without time zone AT TIME ZONE 'utc'::text)) - (bc.time_start AT TIME ZONE 'utc'::text) AS period_length,
    EXTRACT(epoch FROM COALESCE(bc.time_end::timestamp with time zone, ('2023-07-16 00:00:00'::timestamp without time zone AT TIME ZONE 'utc'::text)) - bc.time_start::timestamp with time zone) / (24 * 60 * 60)::numeric AS period_length_days,
    count(*) AS n_trips,
    sum(t.duration) AS total_duration,
    sum(t.dist) AS total_distance
   FROM bikesharing.cities c
     LEFT JOIN bikesharing.bike_city bc ON c.id = bc.city_id
     LEFT JOIN bikesharing.trips t ON t.time_start >= bc.time_start AND t.time_start <= COALESCE(bc.time_end::timestamp with time zone, ('2023-07-16 00:00:00'::timestamp without time zone AT TIME ZONE 'utc'::text)) AND bc.bike_id = t.bike_id
  WHERE c.final_eval AND t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision
  GROUP BY bc.city_id, bc.bike_id, bc.time_start, bc.time_end
  ORDER BY bc.city_id, bc.bike_id
WITH DATA;

-- View indexes:
CREATE INDEX bike_city_usage_city ON bikesharing.bike_city_usage USING btree (city_id);


-- bikesharing.bike_usage source

CREATE MATERIALIZED VIEW bikesharing.bike_usage
TABLESPACE pg_default
AS SELECT bike_trips.bike_id,
    bike_operating_days.city_id,
    bike_trips.trips,
    bike_trips.total_dist,
    bike_operating_days.operating_days
   FROM ( SELECT tl.bike_id,
            count(*) AS trips,
            sum(tl.dist) AS total_dist
           FROM bikesharing.trips_localized tl
          GROUP BY tl.bike_id) bike_trips
     JOIN ( SELECT b.id AS bike_id,
            max(b.city_id) AS city_id,
            count(*)::double precision / (24.0 * 60.0)::double precision AS operating_days
           FROM bikesharing.bikes b
             JOIN bikesharing.scrapes s ON s.time_scrape >= b.first_seen AND s.time_scrape <= b.last_seen AND NOT s.discarded
          GROUP BY b.id) bike_operating_days ON bike_trips.bike_id = bike_operating_days.bike_id
WITH DATA;

-- View indexes:
CREATE INDEX bike_usage_bike_id_idx ON bikesharing.bike_usage USING btree (bike_id);
CREATE INDEX bike_usage_city_id_idx ON bikesharing.bike_usage USING btree (city_id);


-- bikesharing.bike_view source

CREATE OR REPLACE VIEW bikesharing.bike_view
AS SELECT b.id,
    b.bike_type_id,
    b.city_id,
    b.first_seen,
    ls.last_seen,
    b.boardcomputer
   FROM bikesharing.bikes b
     LEFT JOIN ( SELECT DISTINCT ON (bl.bike_id) bl.bike_id,
            bl."time" AS last_seen
           FROM bikesharing.bike_locations bl
          ORDER BY bl.bike_id, bl."time" DESC) ls ON b.id = ls.bike_id;


-- bikesharing.city_areas source

CREATE MATERIALIZED VIEW bikesharing.city_areas
TABLESPACE pg_default
AS WITH dbscan AS (
         SELECT st_clusterdbscan(st_transform(t.geom_start, 3857), eps => 3000::double precision, minpoints => 50) OVER (PARTITION BY t.city_id) AS cid,
            t.geom_start,
            t.city_id
           FROM ( SELECT DISTINCT ON (trips.bike_id, trips.city_id) trips.geom_start,
                    trips.city_id
                   FROM bikesharing.trips
                  WHERE st_distancesphere(st_point(21.012684::double precision, 52.2812::double precision, 4326), trips.geom_start) > 1000::double precision) t
        ), largest_clusters AS (
         SELECT DISTINCT ON (dbscan.city_id) dbscan.city_id,
            dbscan.cid,
            count(*) AS count
           FROM dbscan
          GROUP BY dbscan.city_id, dbscan.cid
          ORDER BY dbscan.city_id, (count(*)) DESC
        ), locations AS (
         SELECT sub.cid,
            sub.geom_start,
            sub.city_id
           FROM ( SELECT dbscan.cid,
                    dbscan.geom_start,
                    dbscan.city_id,
                    dbscan.geom_start::geography <-> st_point(c.lng::double precision, c.lat::double precision, 4326)::geography AS dist,
                    count(dbscan.cid) OVER (PARTITION BY dbscan.city_id) AS cid_count
                   FROM dbscan
                     JOIN bikesharing.cities c ON dbscan.city_id = c.id) sub
          WHERE (sub.cid IS NOT NULL OR sub.cid_count = 0) AND sub.dist < 100000::double precision
        ), geometries AS (
         SELECT locations.city_id,
            st_collect(locations.geom_start) AS points,
            st_transform(st_buffer(st_transform(st_convexhull(st_collect(locations.geom_start)), 3857), 1000::double precision), 4326) AS convexhull
           FROM locations
          GROUP BY locations.city_id
        )
 SELECT geometries.city_id,
    geometries.convexhull AS geom,
    st_area(geometries.convexhull::geography) AS area,
    geometries.points
   FROM geometries
  WHERE (geometries.city_id IN ( SELECT cities.id
           FROM bikesharing.cities
          WHERE cities.final_eval))
  ORDER BY (st_area(geometries.convexhull::geography)) DESC
WITH DATA;


-- bikesharing.city_data source

CREATE OR REPLACE VIEW bikesharing.city_data
AS SELECT c.id AS city_id,
    c.name,
    kpi.nbikes,
    kpi.tdb,
    kpi.kdb,
    elp.pop_2011_01_01 AS population,
    e.cars_registered / elp.pop_2011_01_01::double precision AS cars_per_capita,
    e.median_household_income,
    e.pop_share_5564,
    e.share_pop_75plus,
    e.km_bikelane,
    e.cars_registered,
    e.traffic_deaths,
    e.share_pt,
    e.share_cycling,
    e.unemployment_rate,
    e.transport_infrastructure_km,
    e.pm10_over_treshold_days,
    e.no2_over_treshold_days,
    e.pers_education_idced_5to8,
    e.houseprice_m2,
    e.appartmentprice_m2,
    e.avg_income,
    e.share_pop_15_19,
    e.share_pop_20_24,
    e.share_pop_25_34,
    e.share_pop_35_44,
    e.share_pop_45_54,
    e.share_pop_55_64,
    e.share_pop_65_74,
    e.women_per_100men,
    e.median_age,
    e.student_share_per1000,
    e.share_green_areas,
    e.nr_commuter_in,
    e.nr_commuter_out,
    e.taxi_price,
    e.commute_time_avg,
    e.available_beds_per_1000,
    en.internet_daily,
    en.internet_banking,
    en.tourism_p1000
   FROM bikesharing.cities c
     LEFT JOIN bikesharing.city_lau cl ON c.id = cl.city_id
     LEFT JOIN bikesharing.city_nuts cn ON c.id = cn.id
     LEFT JOIN statistical.eurostat_lau2_pop elp ON cl.cntr_lau_code = elp.cntr_lau_code
     LEFT JOIN bikesharing.city_eurostat_region cer ON c.id = cer.city_id
     LEFT JOIN statistical.eurostat_city_data e ON cer.region = e.region
     LEFT JOIN bikesharing.eurostat_nuts en ON c.id = en.city_id
     LEFT JOIN ( SELECT bu.city_id,
            count(*) AS nbikes,
            sum(bu.trips)::double precision / sum(bu.operating_days) AS tdb,
            sum(bu.total_dist) / sum(bu.operating_days) / 1000::double precision AS kdb
           FROM bikesharing.bike_usage bu
          GROUP BY bu.city_id) kpi ON c.id = kpi.city_id;


-- bikesharing.city_nuts source

CREATE OR REPLACE VIEW bikesharing.city_nuts
AS SELECT cn.id,
    cn.nuts_id AS nuts3,
    "substring"(cn.nuts_id::text, 0, 5)::character varying(5) AS nuts2,
    "substring"(cn.nuts_id::text, 0, 4)::character varying(5) AS nuts1
   FROM bikesharing.city_nuts3 cn;


-- bikesharing.city_nuts3 source

CREATE OR REPLACE VIEW bikesharing.city_nuts3
AS SELECT c.id,
    nm.nuts_id,
    st_point(c.lng::double precision, c.lat::double precision, 4326) AS st_point
   FROM bikesharing.cities c
     LEFT JOIN LATERAL ( SELECT nm_1.gid,
            nm_1.nuts_id,
            nm_1.levl_code,
            nm_1.cntr_code,
            nm_1.name_latn,
            nm_1.nuts_name,
            nm_1.mount_type,
            nm_1.urbn_type,
            nm_1.coast_type,
            nm_1.fid,
            nm_1.geom
           FROM administrative_boundaries.nuts_2021_10m nm_1
          WHERE (st_transform(st_point(c.lng::double precision, c.lat::double precision, 4326), 3035) <-> nm_1.geom) < 15000::double precision AND nm_1.levl_code = 3
          ORDER BY (st_transform(st_point(c.lng::double precision, c.lat::double precision, 4326), 3035) <-> nm_1.geom)
         LIMIT 1) nm ON true
  ORDER BY nm.nuts_id, c.lng;


-- bikesharing.city_populations source

CREATE MATERIALIZED VIEW bikesharing.city_populations
TABLESPACE pg_default
AS SELECT ca.city_id,
    sum((st_summarystats(st_clip(wp.rast, ca.geom))).sum) AS pop
   FROM statistical.worldpop_europe_2020 wp
     RIGHT JOIN bikesharing.city_areas ca ON st_intersects(wp.rast, ca.geom)
  GROUP BY ca.city_id, ca.geom
WITH DATA;


-- bikesharing.city_populism source

CREATE OR REPLACE VIEW bikesharing.city_populism
AS WITH correspondence(nuts2016, nuts2021) AS (
         VALUES ('HR046'::text,'HR061'::text), ('HR044'::text,'HR062'::text), ('HR045'::text,'HR063'::text), ('HR043'::text,'HR064'::text), ('HR042'::text,'HR065'::text), ('HR041'::text,'HR050'::text), ('HR047'::text,'HR021'::text), ('HR048'::text,'HR022'::text), ('HR049'::text,'HR023'::text), ('HR04A'::text,'HR024'::text), ('HR04B'::text,'HR025'::text), ('HR04C'::text,'HR026'::text), ('HR04D'::text,'HR027'::text), ('HR04E'::text,'HR028'::text), ('ES530'::text,'ES531'::text), ('ES530'::text,'ES532'::text), ('ES700'::text,'ES704'::text), ('ES700'::text,'ES705'::text), ('ES700'::text,'ES708'::text), ('SI0'::text,'SI034'::text), ('SI0'::text,'SI035'::text), ('SI0'::text,'SI041'::text), ('SI0'::text,'SI042'::text), ('SI0'::text,'SI043'::text), ('UKM8'::text,'UKM82'::text), ('UKM7'::text,'UKM77'::text), ('UKI7'::text,'UKI74'::text), ('UKJ1'::text,'UKJ12'::text), ('UKJ2'::text,'UKJ25'::text), ('UKK4'::text,'UKK43'::text), ('UKL1'::text,'UKL18'::text), ('UKL2'::text,'UKL22'::text), ('UKN0'::text,'UKN22'::text)
        )
 SELECT c.id,
    neep.nuts2016,
    neep.populist,
    neep.farleft,
    neep.farright,
    neep.eurosceptic,
    neep.nonvoting
   FROM bikesharing.cities c
     JOIN bikesharing.city_nuts3 cn ON c.id = cn.id
     LEFT JOIN correspondence ON cn.nuts_id::text = correspondence.nuts2021
     LEFT JOIN statistical.nuts_ep_elections_populism neep ON COALESCE(correspondence.nuts2016, cn.nuts_id::text) = neep.nuts2016::text
  WHERE c.final_eval
  ORDER BY c.id;


-- bikesharing.eurostat_nuts source

CREATE OR REPLACE VIEW bikesharing.eurostat_nuts
AS SELECT cn.id AS city_id,
    COALESCE(iu11."2022 ", iu11."2021 ", iu11."2020 ", iu11."2019 ", iu12."2022 ", iu12."2021 ", iu12."2020 ", iu12."2019 ") AS internet_banking,
    COALESCE(iu21."2022 ", iu21."2021 ", iu21."2020 ", iu21."2019 ", iu22."2022 ", iu22."2021 ", iu22."2020 ", iu22."2019 ") AS internet_daily,
    COALESCE(t."2019 ", t."2018 ", t."2017 ") AS tourism_p1000
   FROM bikesharing.city_nuts cn
     LEFT JOIN statistical.internet_usage iu11 ON cn.nuts2::text = iu11.geo AND iu11.indic_is = 'I_IUBK'::text AND iu11.unit = 'PC_IND'::text
     LEFT JOIN statistical.internet_usage iu12 ON cn.nuts1::text = iu12.geo AND iu12.indic_is = 'I_IUBK'::text AND iu12.unit = 'PC_IND'::text
     LEFT JOIN statistical.internet_usage iu21 ON cn.nuts2::text = iu21.geo AND iu21.indic_is = 'I_IDAY'::text AND iu21.unit = 'PC_IND'::text
     LEFT JOIN statistical.internet_usage iu22 ON cn.nuts1::text = iu22.geo AND iu22.indic_is = 'I_IDAY'::text AND iu22.unit = 'PC_IND'::text
     LEFT JOIN statistical.tourism t ON cn.nuts2::text = t.geo AND t.c_resid = 'TOTAL'::text AND t.unit = 'P_THAB'::text;


-- bikesharing.intercity source

CREATE MATERIALIZED VIEW bikesharing.intercity
TABLESPACE pg_default
AS SELECT t2.time_start,
    t2.bike_id,
    t2.city_id_start,
    t2.city_id_end
   FROM ( SELECT t.time_start,
            t.bike_id,
            t.city_id AS city_id_start,
            lead(t.city_id, 1) OVER (PARTITION BY t.bike_id ORDER BY t.time_start) AS city_id_end
           FROM bikesharing.trips t) t2
  WHERE t2.city_id_start <> t2.city_id_end AND t2.city_id_start IS NOT NULL AND t2.city_id_end IS NOT NULL
WITH DATA;


-- bikesharing.kpis source

CREATE MATERIALIZED VIEW bikesharing.kpis
TABLESPACE pg_default
AS ( SELECT bcu.city_id,
    NULL::timestamp with time zone AS "timestamp",
    sum(bcu.n_trips) AS n_trips,
    sum(bcu.period_length_days) / (EXTRACT(epoch FROM max(bcu.period_end) - min(bcu.period_start)) / 86400::numeric) AS n_bikes,
    sum(bcu.n_trips) / sum(bcu.period_length_days) AS tdb,
    sum(bcu.total_distance) / 1000::double precision AS total_distance_km,
    sum(bcu.total_distance) / 1000::double precision / sum(bcu.n_trips)::double precision AS avg_distance_km,
    sum(bcu.total_distance) / 1000::double precision / sum(bcu.period_length_days)::double precision AS kdb,
    sum(bcu.total_duration) AS total_duration,
    sum(bcu.total_duration) / sum(bcu.n_trips)::double precision AS avg_duration,
    sum(bcu.total_duration) / sum(bcu.period_length_days)::double precision AS ddb,
    (sum(bcu.n_trips) / (EXTRACT(epoch FROM max(bcu.period_end) - min(bcu.period_start)) / 86400::numeric))::double precision / cp.pop AS tdc
   FROM bikesharing.bike_city_usage bcu
     JOIN bikesharing.city_populations cp ON bcu.city_id = cp.city_id
  GROUP BY bcu.city_id, cp.pop
  ORDER BY bcu.city_id)
UNION
( WITH trip_statistics AS (
         SELECT t.city_id,
            date_trunc('hour'::text, (t.time_start AT TIME ZONE 'utc'::text)) AS "timestamp",
            count(*) AS n_trips,
            sum(t.duration) AS total_duration,
            avg(t.duration) AS avg_duration,
            sum(t.dist) / 1000.0::double precision AS total_distance_km,
            avg(t.dist) / 1000.0::double precision AS avg_distance_km
           FROM bikesharing.cities c
             JOIN bikesharing.trips t ON c.id = t.city_id
          WHERE c.final_eval AND t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision
          GROUP BY t.city_id, (date_trunc('hour'::text, (t.time_start AT TIME ZONE 'utc'::text)))
          ORDER BY t.city_id, (date_trunc('hour'::text, (t.time_start AT TIME ZONE 'utc'::text)))
        ), kpis_initial AS (
         SELECT ts.city_id,
            ts."timestamp",
            ts.n_trips,
            ts.total_duration,
            ts.avg_duration,
            ts.total_distance_km,
            ts.avg_distance_km,
            count(DISTINCT bc.bike_id) AS n_bikes,
            24.0 * ts.n_trips::numeric / count(DISTINCT bc.bike_id)::numeric AS tdb,
            24.0::double precision * ts.total_distance_km / count(DISTINCT bc.bike_id)::double precision AS kdb,
            24.0::double precision * ts.total_duration / count(DISTINCT bc.bike_id)::double precision AS ddb
           FROM trip_statistics ts
             JOIN bikesharing.bike_city bc ON ts.city_id = bc.city_id AND ts."timestamp" >= (bc.time_start AT TIME ZONE 'utc'::text) AND ts."timestamp" <= COALESCE((bc.time_end AT TIME ZONE 'utc'::text), (now() AT TIME ZONE 'utc'::text)::timestamp with time zone)
          GROUP BY ts.city_id, ts."timestamp", ts.n_trips, ts.total_duration, ts.avg_duration, ts.total_distance_km, ts.avg_distance_km
          ORDER BY ts.city_id, ts."timestamp"
        )
 SELECT ki.city_id,
    ki."timestamp",
    ki.n_trips,
    ki.n_bikes,
    ki.tdb,
    ki.total_distance_km,
    ki.avg_distance_km,
    ki.kdb,
    ki.total_duration,
    ki.avg_duration,
    ki.ddb,
    (ki.n_trips * 24)::double precision / cp.pop AS tdc
   FROM kpis_initial ki
     JOIN bikesharing.city_populations cp ON cp.city_id = ki.city_id)
WITH DATA;

-- View indexes:
CREATE INDEX kpis_non_hourly_quick_access ON bikesharing.kpis USING btree ("timestamp") WHERE ("timestamp" IS NULL);


-- bikesharing.kpis_localized source

CREATE OR REPLACE VIEW bikesharing.kpis_localized
AS SELECT k.city_id,
    (k."timestamp" AT TIME ZONE c.timezone) AS "timestamp",
    k.n_trips,
    k.n_bikes,
    k.tdb,
    k.total_distance_km,
    k.avg_distance_km,
    k.kdb,
    k.total_duration,
    k.avg_duration,
    k.ddb,
    k.tdc
   FROM bikesharing.kpis k
     JOIN bikesharing.cities c ON k.city_id = c.id
  ORDER BY (k."timestamp" IS NULL) DESC, k.city_id, k."timestamp";


-- bikesharing.kpis_utc source

CREATE MATERIALIZED VIEW bikesharing.kpis_utc
TABLESPACE pg_default
AS ( SELECT bcu.city_id,
    NULL::timestamp with time zone AS "timestamp",
    sum(bcu.n_trips) AS n_trips,
    sum(bcu.period_length_days) / (EXTRACT(epoch FROM max(bcu.period_end) - min(bcu.period_start)) / 86400::numeric) AS n_bikes,
    sum(bcu.n_trips) / sum(bcu.period_length_days) AS tdb,
    sum(bcu.total_distance) / 1000::double precision AS total_distance_km,
    sum(bcu.total_distance) / 1000::double precision / sum(bcu.n_trips)::double precision AS avg_distance_km,
    sum(bcu.total_distance) / 1000::double precision / sum(bcu.period_length_days)::double precision AS kdb,
    sum(bcu.total_duration) AS total_duration,
    sum(bcu.total_duration) / sum(bcu.n_trips)::double precision AS avg_duration,
    sum(bcu.total_duration) / sum(bcu.period_length_days)::double precision AS ddb,
    (sum(bcu.n_trips) / (EXTRACT(epoch FROM max(bcu.period_end) - min(bcu.period_start)) / 86400::numeric))::double precision / cp.pop AS tdc
   FROM bikesharing.bike_city_usage bcu
     JOIN bikesharing.city_populations cp ON bcu.city_id = cp.city_id
  GROUP BY bcu.city_id, cp.pop
  ORDER BY bcu.city_id)
UNION
( WITH trip_statistics AS (
         SELECT t.city_id,
            date_trunc('hour'::text, t.time_start) AS "timestamp",
            count(*) AS n_trips,
            sum(t.duration) AS total_duration,
            avg(t.duration) AS avg_duration,
            sum(t.dist) / 1000.0::double precision AS total_distance_km,
            avg(t.dist) / 1000.0::double precision AS avg_distance_km
           FROM bikesharing.cities c
             JOIN bikesharing.trips t ON c.id = t.city_id
          WHERE c.final_eval AND t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision
          GROUP BY t.city_id, (date_trunc('hour'::text, t.time_start))
          ORDER BY t.city_id, (date_trunc('hour'::text, t.time_start))
        ), kpis_initial AS (
         SELECT ts.city_id,
            ts."timestamp",
            ts.n_trips,
            ts.total_duration,
            ts.avg_duration,
            ts.total_distance_km,
            ts.avg_distance_km,
            count(DISTINCT bc.bike_id) AS n_bikes,
            24.0 * ts.n_trips::numeric / count(DISTINCT bc.bike_id)::numeric AS tdb,
            24.0::double precision * ts.total_distance_km / count(DISTINCT bc.bike_id)::double precision AS kdb,
            24.0::double precision * ts.total_duration / count(DISTINCT bc.bike_id)::double precision AS ddb
           FROM trip_statistics ts
             JOIN LATERAL ( SELECT bc_1.bike_id,
                    bc_1.city_id,
                    bc_1.time_start,
                    bc_1.time_end
                   FROM bikesharing.bike_city bc_1
                  WHERE ts.city_id = bc_1.city_id AND ts."timestamp" >= bc_1.time_start
                  ORDER BY bc_1.time_start DESC
                 LIMIT 1) bc ON true
          GROUP BY ts.city_id, ts."timestamp", ts.n_trips, ts.total_duration, ts.avg_duration, ts.total_distance_km, ts.avg_distance_km
          ORDER BY ts.city_id, ts."timestamp"
        )
 SELECT ki.city_id,
    ki."timestamp",
    ki.n_trips,
    ki.n_bikes,
    ki.tdb,
    ki.total_distance_km,
    ki.avg_distance_km,
    ki.kdb,
    ki.total_duration,
    ki.avg_duration,
    ki.ddb,
    (ki.n_trips * 24)::double precision / cp.pop AS tdc
   FROM kpis_initial ki
     JOIN bikesharing.city_populations cp ON cp.city_id = ki.city_id)
WITH NO DATA;


-- bikesharing.temp_view_durations source

CREATE OR REPLACE VIEW bikesharing.temp_view_durations
AS WITH durations AS (
         SELECT EXTRACT(epoch FROM t.duration) AS duration_seconds,
            floor(EXTRACT(epoch FROM t.duration) / 3600::numeric) AS duration_hours,
            floor(EXTRACT(epoch FROM t.duration) / 60::numeric) % 60::numeric AS duration_minutes,
                CASE
                    WHEN c.return_to_official_only = false THEN 1
                    ELSE 0
                END AS is_freefloating,
                CASE
                    WHEN c.return_to_official_only = true THEN 1
                    ELSE 0
                END AS is_stationbased,
                CASE
                    WHEN bt.propulsion_type = 'electric_assist'::text THEN 1
                    ELSE 0
                END AS is_electric,
                CASE
                    WHEN bt.propulsion_type = 'human'::text THEN 1
                    ELSE 0
                END AS is_human,
                CASE
                    WHEN EXTRACT(dow FROM t.time_start) = ANY (ARRAY[0::numeric, 6::numeric]) THEN 1
                    ELSE 0
                END AS is_weekend,
                CASE
                    WHEN EXTRACT(dow FROM t.time_start) <> ALL (ARRAY[0::numeric, 6::numeric]) THEN 1
                    ELSE 0
                END AS is_weekday
           FROM bikesharing.trips_localized t
             JOIN bikesharing.bikes b ON t.bike_id = b.id
             JOIN bikesharing.bike_types bt ON b.bike_type_id = bt.id
             JOIN bikesharing.cities c ON t.city_id = c.id
        ), category_counts AS (
         SELECT durations.duration_hours,
            durations.duration_minutes,
            sum(durations.is_freefloating) AS freefloating_count,
            sum(durations.is_stationbased) AS stationbased_count,
            sum(durations.is_electric) AS electric_count,
            sum(durations.is_human) AS human_count,
            sum(durations.is_weekend) AS weekend_count,
            sum(durations.is_weekday) AS weekday_count
           FROM durations
          WHERE durations.duration_hours <= 2::numeric
          GROUP BY durations.duration_hours, durations.duration_minutes
        )
 SELECT category_counts.duration_hours,
    category_counts.duration_minutes AS total_duration_minutes,
    category_counts.freefloating_count::numeric / sum(category_counts.freefloating_count) OVER () AS freefloating_share,
    category_counts.stationbased_count::numeric / sum(category_counts.stationbased_count) OVER () AS stationbased_share,
    category_counts.electric_count::numeric / sum(category_counts.electric_count) OVER () AS electric_share,
    category_counts.human_count::numeric / sum(category_counts.human_count) OVER () AS human_share,
    category_counts.weekend_count::numeric / sum(category_counts.weekend_count) OVER () AS weekend_share,
    category_counts.weekday_count::numeric / sum(category_counts.weekday_count) OVER () AS weekday_share
   FROM category_counts
  ORDER BY category_counts.duration_hours, category_counts.duration_minutes;


-- bikesharing.trips source

CREATE MATERIALIZED VIEW bikesharing.trips
TABLESPACE pg_default
AS SELECT t2.trip_id,
    t2.bike_id,
    t2.city_id,
    t2.time_start,
    t2.time_end,
    t2.geom_start,
    t2.geom_end,
    t2.station_id_start,
    t2.station_id_end,
    t2.battery_start,
    t2.battery_end,
    t2.time_end - t2.time_start AS duration,
    t2.dist
   FROM ( SELECT row_number() OVER () AS trip_id,
            t.bike_id,
            bc.city_id,
            t.time_start,
            t.time_end,
            t.geom_start,
            t.geom_end,
            t.station_id_start,
            t.station_id_end,
            t.battery_start,
            t.battery_end,
            t.dist
           FROM ( SELECT bl.bike_id,
                    lag(bl."time", 0) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS time_end,
                    lag(bl."time", 1) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS time_start,
                    lag(bl.geom, 1) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS geom_start,
                    lag(bl.geom, 0) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS geom_end,
                    lag(bl.station_id, 1) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS station_id_start,
                    lag(bl.station_id, 0) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS station_id_end,
                    lag(bl.pedelec_battery, 1) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS battery_start,
                    lag(bl.pedelec_battery, 0) OVER (PARTITION BY bl.bike_id ORDER BY bl."time") AS battery_end,
                    st_distancesphere(bl.geom, lag(bl.geom, 1) OVER (PARTITION BY bl.bike_id ORDER BY bl."time")) AS dist
                   FROM bikesharing.bike_locations bl) t
             LEFT JOIN LATERAL ( SELECT bc_1.city_id
                   FROM bikesharing.bike_city bc_1
                  WHERE t.time_start >= bc_1.time_start AND bc_1.bike_id = t.bike_id
                  ORDER BY bc_1.time_start DESC
                 LIMIT 1) bc ON true
          WHERE t.dist > 10::double precision) t2
  WHERE (t2.time_end - t2.time_start) > '00:02:00'::interval
WITH DATA;

-- View indexes:
CREATE INDEX trips_bike_city_idx ON bikesharing.trips USING btree (bike_id, city_id);
CREATE INDEX trips_bike_id_idx ON bikesharing.trips USING btree (bike_id);
CREATE INDEX trips_bike_id_time_idx ON bikesharing.trips USING btree (bike_id, time_start);
CREATE INDEX trips_dist_duration_idx ON bikesharing.trips USING btree (duration, dist);
CREATE INDEX trips_dist_idx ON bikesharing.trips USING btree (dist);
CREATE INDEX trips_duration_idx ON bikesharing.trips USING btree (duration);
CREATE INDEX trips_geom_start_idx ON bikesharing.trips USING gist (geom_start, geom_end);
CREATE INDEX trips_time_start_idx ON bikesharing.trips USING btree (time_start);
CREATE INDEX trips_time_start_idx_2 ON bikesharing.trips USING brin (time_start);


-- bikesharing.trips_export source

CREATE MATERIALIZED VIEW bikesharing.trips_export
TABLESPACE pg_default
AS SELECT t.trip_id,
    t.bike_id,
    t.city_id,
    EXTRACT(epoch FROM t.time_start) AS time_start,
    EXTRACT(epoch FROM t.time_end) AS time_end,
    st_x(t.geom_start) AS lon_start,
    st_y(t.geom_start) AS lat_start,
    st_x(t.geom_end) AS lon_end,
    st_y(t.geom_end) AS lat_end,
    t.station_id_start,
    t.station_id_end,
    t.battery_start,
    t.battery_end,
    EXTRACT(epoch FROM t.duration) AS duration,
    t.dist AS distance
   FROM bikesharing.trips t
  WHERE t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision AND (t.city_id IN ( SELECT c.id
           FROM bikesharing.cities c
          WHERE c.final_eval))
WITH DATA;

-- View indexes:
CREATE INDEX trips_export_bike_id_idx ON bikesharing.trips_export USING btree (bike_id);
CREATE INDEX trips_export_city_id_idx ON bikesharing.trips_export USING btree (city_id);


-- bikesharing.trips_localized source

CREATE OR REPLACE VIEW bikesharing.trips_localized
AS SELECT t.trip_id,
    t.bike_id,
    t.city_id,
    timezone(c.timezone, timezone('utc'::text, t.time_start)) AS time_start,
    timezone(c.timezone, timezone('utc'::text, t.time_end)) AS time_end,
    t.geom_start,
    t.geom_end,
    t.station_id_start,
    t.station_id_end,
    t.battery_start,
    t.battery_end,
    t.duration,
    t.dist
   FROM bikesharing.trips t
     JOIN bikesharing.bikes b ON t.bike_id = b.id
     JOIN bikesharing.cities c ON b.city_id = c.id
  WHERE t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision;


-- bikesharing.trips_localized_filtered source

CREATE OR REPLACE VIEW bikesharing.trips_localized_filtered
AS SELECT t.trip_id,
    t.bike_id,
    t.city_id,
    t.time_start,
    t.time_end,
    t.geom_start,
    t.geom_end,
    t.station_id_start,
    t.station_id_end,
    t.battery_start,
    t.battery_end,
    t.duration,
    t.dist
   FROM bikesharing.trips_localized t
  WHERE t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision AND (t.city_id IN ( SELECT c.id
           FROM bikesharing.cities c
          WHERE c.final_eval));


-- bikesharing.trips_localized_filtered_mv source

CREATE MATERIALIZED VIEW bikesharing.trips_localized_filtered_mv
TABLESPACE pg_default
AS SELECT t.trip_id,
    t.bike_id,
    t.city_id,
    t.time_start,
    t.time_end,
    t.geom_start,
    t.geom_end,
    t.station_id_start,
    t.station_id_end,
    t.battery_start,
    t.battery_end,
    t.duration,
    t.dist
   FROM bikesharing.trips_localized t
  WHERE t.dist > 50::double precision AND t.dist < 20000::double precision AND t.duration > '00:01:30'::interval AND t.duration < '08:00:00'::interval AND st_x(t.geom_start) <= 180::double precision AND st_x(t.geom_start) >= '-180'::integer::double precision AND st_y(t.geom_start) < 90::double precision AND st_y(t.geom_start) > '-90'::integer::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) >= 0.25::double precision AND (3.6::double precision * (t.dist / EXTRACT(epoch FROM t.duration)::double precision)) <= 15.05::double precision AND (t.city_id IN ( SELECT c.id
           FROM bikesharing.cities c
          WHERE c.final_eval))
WITH DATA;


-- bikesharing.year_td_stddev_ff_sb source

CREATE MATERIALIZED VIEW bikesharing.year_td_stddev_ff_sb
TABLESPACE pg_default
AS SELECT t.return_to_official_only,
    date_trunc('month'::text, t."time") AS date_trunc,
    avg(t.trips) AS avg,
    stddev(t.trips) AS stddev
   FROM ( SELECT count(*) AS trips,
            date_trunc('day'::text, tl.time_start) AS "time",
            c.return_to_official_only
           FROM bikesharing.trips tl
             JOIN bikesharing.cities c ON tl.city_id = c.id
          GROUP BY (date_trunc('day'::text, tl.time_start)), c.return_to_official_only) t
  GROUP BY t.return_to_official_only, (date_trunc('month'::text, t."time"))
WITH DATA;


-- bikesharing.year_tdb_stddev_ff_sb source

CREATE MATERIALIZED VIEW bikesharing.year_tdb_stddev_ff_sb
TABLESPACE pg_default
AS SELECT t.return_to_official_only,
    date_trunc('month'::text, t."time") AS date_trunc,
    avg(t.dist) AS avg,
    stddev(t.dist) AS stddev
   FROM ( SELECT date_trunc('day'::text, tl.time_start) AS "time",
            tl.dist,
            c.return_to_official_only
           FROM bikesharing.trips tl
             JOIN bikesharing.cities c ON tl.city_id = c.id) t
  GROUP BY t.return_to_official_only, (date_trunc('month'::text, t."time"))
WITH DATA;



-- DROP FUNCTION bikesharing.bike_city_copy();

CREATE OR REPLACE FUNCTION bikesharing.bike_city_copy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.bike_city
              (bike_id, city_id, time_start, time_end)
              VALUES (NEW."bike_id",
              		  new."city_id",
              		  new."time",
              		 null);
  update bikesharing.bike_city
              set "time_end" = new.time
              where bike_id = new.bike_id and city_id = old.city_id and time_end is null;
  RETURN NEW;
END;
$function$
;

-- DROP FUNCTION bikesharing.bike_city_copy_ins();

CREATE OR REPLACE FUNCTION bikesharing.bike_city_copy_ins()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.bike_city
              (bike_id, city_id, time_start, time_end)
              VALUES (NEW."bike_id",
              		  new."city_id",
              		  new."time",
              		 null);
  RETURN NEW;
END;
$function$
;

-- DROP FUNCTION bikesharing.bike_city_prod_copy();

CREATE OR REPLACE FUNCTION bikesharing.bike_city_prod_copy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.bike_city
              (bike_id, city_id, time_start, time_end)
              VALUES (NEW."id",
              		  new."city_id",
              		  new."last_seen",
              		 null);
  update bikesharing.bike_city
              set "time_end" = new."last_seen"
              where bike_id = new.id and city_id = old.city_id and time_end is null;
  RETURN NEW;
END;
$function$
;

-- DROP FUNCTION bikesharing.bike_city_prod_copy_ins();

CREATE OR REPLACE FUNCTION bikesharing.bike_city_prod_copy_ins()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.bike_city
              (bike_id, city_id, time_start, time_end)
              VALUES (NEW."id",
              		  new."city_id",
              		  new."last_seen",
              		 null);
  RETURN NEW;
END;
$function$
;

-- DROP FUNCTION bikesharing.bike_loc_copy();

CREATE OR REPLACE FUNCTION bikesharing.bike_loc_copy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.bike_locations
              ("bike_id", "time", "station_id", "geom", "pedelec_battery")
              VALUES (NEW."bike_id",
              		  new."time",
              		  new."station_id",
              		  new."geom",
              		  new."pedelec_battery");
  INSERT INTO bikesharing.bike_locations
              ("bike_id", "time", "station_id", "geom", "pedelec_battery")
              select "bike_id",
              		  "time",
              		  "station_id",
              		  "geom",
              		  "pedelec_battery" from bikesharing.bike_locations_temp_pre where bike_id = new.bike_id
              		 on conflict on constraint bike_locations_pk do nothing;
  RETURN NEW;
END;
$function$
;

-- DROP FUNCTION bikesharing.station_status_copy();

CREATE OR REPLACE FUNCTION bikesharing.station_status_copy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO bikesharing.station_status
              ("station_id", "time", "bikes", "booked_bikes", "bikes_available_to_rent", "free_racks", "free_special_racks", "maintenance")
              VALUES (NEW."station_id",
              		  new."time",
              		  new."bikes",
              		  new."booked_bikes",
              		  new."bikes_available_to_rent",
              		  new."free_racks",
              		  new."free_special_racks",
              		 new."maintenance");
  RETURN NEW;
END;
$function$
;
