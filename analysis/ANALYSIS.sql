--1. List countries with income level of "Upper middle income".

SELECT country_name FROM countrystats.country_raw WHERE income_level_id='UMC';

--2. List countries with income level of "Low income" per region.

SELECT 
region_value AS region, country_name 
FROM countrystats.country_raw 
WHERE income_level_id='LIC';

--3. Find the region with the highest proportion of "High income" countries.

WITH hic_count AS(
	SELECT 
	region_value AS region, count(country_name) AS country_count
	FROM countrystats.country_raw 
	WHERE income_level_id='HIC'
	group by region_value)	
SELECT region FROM  hic_count WHERE country_count = (SELECT MAX(country_count) FROM hic_count);

--4. Calculate cumulative/running value of GDP per region ordered by income from
lowest to highest and country name.


SELECT region_value AS region, country_name, income_indicator,
SUM(year_2019) 
OVER(PARTITION BY region_value ORDER BY income_indicator, country_name) AS cumulative_gdp_2019,
SUM(year_2020) 
OVER(PARTITION BY region_value ORDER BY income_indicator, country_name) AS cumulative_gdp_2020,
SUM(year_2021) 
OVER(PARTITION BY region_value ORDER BY income_indicator, country_name) AS cumulative_gdp_2021,
SUM(year_2022) 
OVER(PARTITION BY region_value ORDER BY income_indicator, country_name) AS cumulative_gdp_2022,
SUM(year_2023) 
OVER(PARTITION BY region_value ORDER BY income_indicator, country_name) AS cumulative_gdp_2023
FROM
(
SELECT country_code, year_2019, year_2020, year_2021, year_2022, year_2023  
FROM countrystats.country_gdp
) gdp 
JOIN
(
SELECT region_value, country_id , country_name,
	CASE 
	WHEN income_level_id='HIC' THEN 5
	WHEN income_level_id='UMC' THEN 4
	WHEN income_level_id='LMC' THEN 3
	WHEN income_level_id='LIC' THEN 2
	WHEN income_level_id='NA' THEN 1
	END AS income_indicator	
FROM countrystats.country_raw 
)country
on country.country_id=gdp.country_code;


--5. Calculate percentage difference in value of GDP year-on-year per country.

SELECT country_name, year_2019 ,year_2020, 
CASE 
WHEN year_2019 > year_2020 
THEN CONCAT('-',TO_CHAR( ROUND( ABS( (year_2020 - year_2019) * 100 / CASE WHEN year_2019 = 0 THEN 1 ELSE year_2019 END), 2), 'FM999999.99'), '%') 
WHEN year_2019 < year_2020 
THEN CONCAT('+',TO_CHAR(ROUND(ABS( (year_2020 - year_2019) * 100 / CASE WHEN year_2019 = 0 THEN 1 ELSE year_2019 END), 2), 'FM999999.99'), '%')
WHEN year_2019 = year_2020 THEN '0%' ELSE 'NA' END
AS percentage_diff_gdp_2019_2020,
CASE 
WHEN year_2020 > year_2021 
THEN CONCAT('-',TO_CHAR( ROUND( ABS( (year_2021 - year_2020) * 100 / CASE WHEN year_2020 = 0 THEN 1 ELSE year_2020 END), 2), 'FM999999.99'), '%') 
WHEN year_2020 < year_2021 
THEN CONCAT('+',TO_CHAR(ROUND(ABS( (year_2021 - year_2020) * 100 / CASE WHEN year_2020 = 0 THEN 1 ELSE year_2020 END), 2), 'FM999999.99'), '%')
WHEN year_2020 = year_2021 THEN '0%' ELSE 'NA' END
AS percentage_diff_gdp_2020_2021,
CASE 
WHEN year_2021 > year_2022 
THEN CONCAT('-',TO_CHAR( ROUND( ABS( (year_2022 - year_2021) * 100 / CASE WHEN year_2021 = 0 THEN 1 ELSE year_2021 END), 2), 'FM999999.99'), '%') 
WHEN year_2021 < year_2022 
THEN CONCAT('+',TO_CHAR(ROUND(ABS( (year_2022 - year_2021) * 100 / CASE WHEN year_2021 = 0 THEN 1 ELSE year_2021 END), 2), 'FM999999.99'), '%')
WHEN year_2021 = year_2022 THEN '0%' ELSE 'NA' END
AS percentage_diff_gdp_2021_2022,
CASE 
WHEN year_2022 > year_2023 
THEN CONCAT('-',TO_CHAR( ROUND( ABS( (year_2023 - year_2022) * 100 / CASE WHEN year_2022 = 0 THEN 1 ELSE year_2022 END), 2), 'FM999999.99'), '%') 
WHEN year_2022 < year_2023 
THEN CONCAT('+',TO_CHAR(ROUND(ABS( (year_2023 - year_2022) * 100 / CASE WHEN year_2022 = 0 THEN 1 ELSE year_2022 END), 2), 'FM999999.99'), '%')
WHEN year_2022 = year_2023 THEN '0%' ELSE 'NA'END
AS percentage_diff_gdp_2022_2023
FROM
countrystats.country_gdp;


--6. List 3 countries with lowest GDP per region.

WITH total_gdp_staus AS(
SELECT region_value AS region, country_name, total_gdp,
ROW_NUMBER() OVER(PARTITION BY region_value ORDER BY total_gdp) as gcp_country_status
FROM
(
SELECT country_code, (year_2019 + year_2020 + year_2021 + year_2022 + year_2023) as total_gdp
FROM countrystats.country_gdp
WHERE year_2019 IS NOT NULL AND  year_2020 IS NOT NULL
AND year_2021 IS NOT NULL AND year_2022 IS NOT NULL
AND year_2023 IS NOT NULL
) gdp 
JOIN
(
SELECT region_value , country_id , country_name
FROM countrystats.country_raw 
)country
on country.country_id=gdp.country_code
)
SELECT region, country_name, total_gdp from total_gdp_staus where gcp_country_status<=3
;


--7. Provide an interesting fact from the dataset.
/*
a. We can see a significant decrease in the GDP of many countries in 2020 due to COVID

b. The GDP dataset doesnot contain the data for all the countries in the Country dataset, So the corresponding analysis is done for the matching countries

c. With the data provided, I was able to build a small Datawarehouse having a STAR schema which can be used for easy integration with downstream applications

*/

-- Challenges
/*
a. While reading the GCP CSV in pandas, it had some unknow column containing null values, so have to cleanse it.

b. Had to change schema as per naming standards so as to maintain proper schema in postgres and to insert data into the table.
Also could have directly created the table out of the pandas data frame but that would result in unconventional naming of tables and columns in the db.

c. In real time scenario, the  password  for the DB could be retrieved from a Key Management Store(KMS) in a secure way, also not using getpass() as it would not work in IDE. So, getting it as input from the command line as of now.

d. The GDP dataset has some missing GDP values which had to be filtered out for certain analysis.

*/
