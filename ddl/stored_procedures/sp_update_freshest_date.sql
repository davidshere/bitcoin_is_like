CREATE OR REPLACE FUNCTION sp_updated_freshest_date() RETURNS void as

$BODY$

BEGIN

	UPDATE
		dim_series
	SET 
		last_updated = latest_dates.latest_value
	FROM (

		SELECT
			series_id, max(cust_series.date) as latest_value
		FROM 
			cust_series
		GROUP BY
			series_id
	) as latest_dates
	WHERE
		latest_dates.series_id = dim_series.id	;

END;

$BODY$

LANGUAGE 'plpgsql';