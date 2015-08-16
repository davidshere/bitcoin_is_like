CREATE OR REPLACE FUNCTION sp_add_bitcoin_data_to_dim_series() RETURNS void AS

$BODY$ 

BEGIN

	INSERT INTO dim_series (
		quandl_code,
		source_code,
		code,
		series_name,
		description)
	VALUES (
		NULL,
		'bitcoinaverage',
		NULL,
		'Bitcoin',
		'Bitcoin is an implementation of a distributed record-keeping technology called the blockchain. It can be used to make financial transacations without the intermediation of a financial institution.'
	)

	;

END;

$BODY$

LANGUAGE 'plpgsql'
;
