CREATE UNIQUE INDEX ix_start_end_dates ON fact_match using btree (start_date, end_date);
CREATE INDEX ix_series on cust_series using btree (series_id);