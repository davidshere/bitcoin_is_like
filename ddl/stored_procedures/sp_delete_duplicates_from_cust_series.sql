CREATE OR REPLACE FUNCTION sp_delete_duplicates_from_cust_series() RETURNS void as

$BODY$

BEGIN

WITH duplicates AS (
	SELECT 	
		series_id, 
		date, 
		max(id) AS latest_id,
		count(*) AS num_duplicates
	FROM 
		cust_series 
	GROUP BY
		1, 2
	HAVING count(*) > 1
), combined_data AS (
	SELECT 
		series.id,
		series.series_id,
		series.date,
		series.value,
		duplicates.latest_id,
		duplicates.num_duplicates
	FROM
		cust_series AS series
	join
		duplicates ON duplicates.date = series.date AND duplicates.series_id = series.series_id
	order by 
		series.series_id, 
		series.date
), ids_to_delete AS (
	SELECT 
		id
	FROM 
		combined_data
	where 
		id != latest_id
)
DELETE FROM 
	cust_series
WHERE
	id IN (SELECT * FROM ids_to_delete)
;

END;

$BODY$

LANGUAGE 'plpgsql'