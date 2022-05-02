
-- DROP SCHEMA countrystats CASCADE;
CREATE SCHEMA countrystats;

SET search_path TO pg_catalog,public,countrystats;


-- DROP TABLE countrystats.country_fact;
CREATE TABLE countrystats.country_fact(
	country_id char(3) NOT NULL ,
	region_id text DEFAULT 'NA',
	admin_region_id text,
	income_level_id text DEFAULT 'NA',
	lending_type_id text,
	CONSTRAINT pk_country_id PRIMARY KEY (country_id)

);


-- DROP TABLE countrystats.country_details;
CREATE TABLE countrystats.country_details(
	country_id char(3) NOT NULL ,
	iso2_code text,
	country_name text,
	capital_city text,
	longitude numeric,
	latitude numeric,
	region_id text,
	CONSTRAINT pk_country_details_id PRIMARY KEY (country_id)

);

-- ALTER TABLE countrystats.country_details DROP CONSTRAINT country_detail_fk;
ALTER TABLE countrystats.country_details ADD CONSTRAINT country_detail_fk FOREIGN KEY (country_id)
REFERENCES countrystats.country_fact (country_id) ;


-- DROP TABLE countrystats.region;
CREATE TABLE countrystats.region(
	region_id text NOT NULL ,
	region_iso2Code text DEFAULT 'NA',
	region_value text DEFAULT 'Aggregates',
	CONSTRAINT pk_region_id PRIMARY KEY (region_id)

);

-- ALTER TABLE countrystats.country_fact DROP CONSTRAINT country_region_fk;
ALTER TABLE countrystats.country_fact ADD CONSTRAINT country_region_fk FOREIGN KEY (region_id)
REFERENCES countrystats.region (region_id) ;


-- DROP TABLE countrystats.admin_region;
CREATE TABLE countrystats.admin_region(
	admin_region_id text NOT NULL ,
	admin_region_iso2Code text DEFAULT 'NA',
	admin_region_value text DEFAULT 'Aggregates',
	CONSTRAINT pk_admin_region_id PRIMARY KEY (admin_region_id)

);

-- ALTER TABLE countrystats.country_fact DROP CONSTRAINT country_admin_region_fk;
ALTER TABLE countrystats.country_fact ADD CONSTRAINT country_admin_region_fk FOREIGN KEY (admin_region_id)
REFERENCES countrystats.admin_region (admin_region_id) ;


-- DROP TABLE countrystats.income_level;
CREATE TABLE countrystats.income_level(
	income_level_id text NOT NULL ,
	income_level_iso2Code text DEFAULT 'NA',
	income_level_value text DEFAULT 'Aggregates',
	CONSTRAINT pk_income_level_id PRIMARY KEY (income_level_id)

);

-- ALTER TABLE countrystats.country_fact DROP CONSTRAINT country_income_level_fk;
ALTER TABLE countrystats.country_fact ADD CONSTRAINT country_income_level_fk FOREIGN KEY (income_level_id)
REFERENCES countrystats.income_level (income_level_id) ;


-- DROP TABLE countrystats.lending_type;
CREATE TABLE countrystats.lending_type(
	lending_type_id text NOT NULL ,
	lending_type_iso2Code text DEFAULT 'NA',
	lending_type_value text DEFAULT 'Aggregates',
	CONSTRAINT pk_lending_type_id PRIMARY KEY (lending_type_id)

);

-- ALTER TABLE countrystats.country_fact DROP CONSTRAINT country_lending_type_fk;
ALTER TABLE countrystats.country_fact ADD CONSTRAINT country_lending_type_fk FOREIGN KEY (lending_type_id)
REFERENCES countrystats.lending_type (lending_type_id) ;


-- DROP TABLE countrystats.country_gdp;
CREATE TABLE countrystats.country_gdp(
	country_code char(3) NOT NULL ,
	country_name text,
	indicator_name text,
	indicator_code text,
	year_2019 numeric,
	year_2020 numeric,
	year_2021 numeric,
	year_2022 numeric,
	year_2023 numeric,
	CONSTRAINT pk_country_gdp_country_code PRIMARY KEY (country_code)

);


-- DROP TABLE countrystats.country_raw;
CREATE TABLE countrystats.country_raw(
	country_id char(3) NOT NULL ,
	iso2_code text,
	country_name text,
	capital_city text,
	Latitude numeric,
	Longitude numeric,
	region_id text,
	region_iso2Code text DEFAULT 'NA',
	region_value text DEFAULT 'Aggregates',
	admin_region_id text ,
	admin_region_iso2Code text DEFAULT 'NA',
	admin_region_value text DEFAULT 'Aggregates',
	income_level_id text ,
	income_level_iso2Code text DEFAULT 'NA',
	income_level_value text DEFAULT 'Aggregates',
	lending_type_id text ,
	lending_type_iso2Code text DEFAULT 'NA',
	lending_type_value text DEFAULT 'Aggregates',
	CONSTRAINT pk_country_raw_id PRIMARY KEY (country_id)

);


