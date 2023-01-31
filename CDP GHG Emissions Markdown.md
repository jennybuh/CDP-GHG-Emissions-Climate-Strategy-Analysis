```sql
## Created tables in this format to load emission data for various report years (2017-2022)

CREATE TABLE 2022_em
(
    org_number INT,
    org_name TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    em_Col_Number INT,
    em_Col_Name TEXT,
    total_em VARCHAR(25),
    inv_year VARCHAR(5),
);
    

## Created table for city's logistics on plan implementation

CREATE TABLE city_targets
(
    org_number int,
    org_name text,
    city text,
    Country text,
    target_type text,
    sector text,
    base_year varchar(5),
    year_of_target_intro varchar(5),
    base_year_em varchar(15),
    percent_reduction_target varchar(5),
    target_year varchar(5),
    target_year_goal_em varchar(15)
);



## Created table for city's plan climate strategy details 


CREATE TABLE 2022_plans
(
    count TEXT(50),
    org_number INT,
    org_name TEXT(50),
    state TEXT(5),
    city TEXT(50),
    plan_financing TEXT(225),
    Plan_authors TEXT(120),
    formal_year_approval TEXT(25),
    total_implementation_costs TEXT(25),
    sectors_covered TEXT(700)
);




## Check number of records available for each org, based on year -- checking to see how many orgs that can be analyzed


SELECT 
	inv_year,
COUNT(DISTINCT Org_number) AS "Num_of_Records"
FROM
    ghg.2022_em
GROUP BY
    inv_year
ORDER BY
    inv_year DESC;
 

## Aggregation across aggregate and non-aggregate columns in queries, run on start on MySQL


SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
 




## Creating unioned table to combine emission data between report years, summing emission data for each table with year range between 2017-2022


CREATE TEMPORARY TABLE org_em
SELECT distinct inv_year,
    org_number,
    org_name,
    state,
    city,
    report_year,
    SUM(total_em) AS calculated_total_em
from
    ghg.2022_em
WHERE
    inv_year >= 2017 AND
    inv_year <= 2022
group by
    org_number
    
UNION ALL

SELECT distinct inv_year,
    org_number,
    org_name,
    state,
    city,
    report_year,
    SUM(total_em) AS calculated_total_em
from
    ghg.2021_em
WHERE
    inv_year >= 2017 AND
    inv_year <= 2021
group by
    org_number
    
UNION ALL

SELECT distinct inv_year,
    org_number,
    org_name,
    state,
    city,
    report_year,
    SUM(total_em) AS calculated_total_em
from
    ghg.2020_em
WHERE
    inv_year >= 2017 AND
    inv_year <= 2020
group by
    org_number
    
UNION ALL

SELECT distinct inv_year,
    org_number,
    org_name,
    state,
    city,
    report_year,
    SUM(total_em) AS calculated_total_em
from
    ghg.2019_em
WHERE
    inv_year >= 2017 AND
    inv_year <= 2019
group by
    org_number
    
UNION ALL

SELECT distinct inv_year,
    org_number,
    org_name,
    state,
    city,
    report_year,
    SUM(total_em) AS calculated_total_em
from
    ghg.2018_em
WHERE
    inv_year >= 2017 AND
    inv_year <= 2018
group by
    org_number;



/* Validating/cleaning data for city targets table, some columns are 0/ NULL based on target type or sector type, as some categories don't require input on year/emission values. Each city has multiple year and emission values based on the sector they're targeting. Combining all emissions but pulling 1 value for each year column. */

## Self-union on city targets test table, pulled specific values for target_types that rely on baseline emissions, left target type wuth Fixed amts as to adjust further down the line

## Left Join - Added the year their climate plans were introduced from the 2022_plans table

CREATE TEMPORARY TABLE tcity_targets
SELECT
	t.org_number,
	t.target_type,
	t.sector,
	p.formal_year_apprival AS year_of_target_intro,
	MAX(t.base_year) as base_year,
	SUM(t.base_year_em) as base_year_em,
	MAX(t.target_year) as target_year,
	SUM(t.target_year_em_goal) as target_year_em_goal
FROM
	ghg.city_targets t
LEFT JOIN
	2022_plans p ON p.org_number = t.org_number
WHERE
    target_type LIKE "Base%"
GROUP BY
	org_number
	
UNION ALL

SELECT
	t.org_number,
	t.target_type,
	t.sector,
	p.formal_year_apprival AS year_of_target_intro,
	MAX(t.base_year) as base_year,
	SUM(t.base_year_em) as base_year_em,
	MAX(t.target_year) as target_year,
	SUM(t.target_year_em_goal) as target_year_em_goal
FROM 
	ghg.city_targets t
LEFT JOIN
	2022_plans p ON p.org_number = t.org_number
WHERE
	target_type LIKE "Fixed%";

## Validating/cleaning values for city's base year/target years
## Updated base_year values for fixed level target, where blank using next closest value (year of target intro)

UPDATE tcity_targets
SET base_year = 
	CASE WHEN base_year = "" AND target_type ="Fixed level target" THEN (year_of_target_intro) END;

## Where year of target intro and target year are the same, set the year of target intro to the NULL. Cannot have a plan start on X year, and also end on the same year.

UPDATE tcity_targets
SET target_year_em_goal = null
WHERE
    target_year_em_goal = "";


## Combining city climate plan targets with city emissions for desginated years, final table for analysis regarding emissions. Cleanup to follow 

CREATE TABLE org_em_targets
SELECT
    e.*,
    t.base_year,
    t.base_year_em,
    t.year_of_target_intro,
    t.target_year,
    t.target_year_em_goal
FROM
	org_range_em e
LEFT JOIN
    tcity_targets t ON e.org_number = t.org_number;
    

## Identify distinctive emissions for each group that isn't 0 or NULL, and queried groups that had less than 2 points of data, removed these orgs from final analysis

SELECT
    org_number,
    org_name,
    COUNT(DISTINCT calculated_total_em) AS em_counts
FROM
    org_em_targets
WHERE
    calculated_total_em != 0 AND
    calculated_total_em IS NOT NULL
GROUP BY
    org_number
HAVING 
    COUNT(DISTINCT calculated_total_em) < 2;

## Identify groups with values of only 0 for emission amounts, added sum column to confirm their totals, removed these orgs from final analysis

 SELECT
    org_number,
    org_name,
    COUNT(DISTINCT calculated_total_em) AS em_counts,
    SUM(calculated_total_em) AS SUM
FROM
    org_em_targets
GROUP BY
    org_number
HAVING 
     SUM(calculated_total_em) = "0";
     
## City's climate strategy details and plans 

/*
Combining finalized emissions/year/target table with city plan details table. 
Org_em_targets table now has less organizations than the city plans table, which has not had any values removed. 

Left joining on the plans table because each org can have multiple rows of values, want to keep this data in tact.

Identifying the columns that need to be removed from the plans table with "remove_#".

The orgs that should not be kept on the table since they were previously removed for having less than 2 points of emission data will show up as NULL values left on org_em_targets columns

*/

CREATE TABLE 2022_em_plans
SELECT
	t.org_name,
	t.state,
	t.city,
	t.year_of_target_intro
	p.plan_financing,
	p.plan_authors,
	p.total_implementation_costs,
	p.org_number AS remove_1,
	p.org_name AS remove_2,
	p.state AS remove_3,
	p.city AS remove_4
FROM
	2022_plans p
LEFT JOIN
	org_em_targets t ON p.org_number = e.org_number;
	

## Removing the dummied columns from the table, now there is only one org_number column

ALTER TABLE 2022_em_plans
DROP remove_1,
DROP remove_2,
DROP remove_3,
DROP remove_4;

## Removing the rows where orgs no longer exist for analysis 

DELETE FROM 2022_em_plans
WHERE
	org_number IS NULL;
	

/* All tables pivoted as needed in Excel for upload in tableau */





```

