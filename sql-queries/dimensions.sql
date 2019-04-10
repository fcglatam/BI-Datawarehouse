

DROP TABLE if exists calendar;

CREATE TABLE calendar
(
  date_id               INT NOT NULL,
  a_date                DATE NOT NULL,
  epoch                 BIGINT NOT NULL,
  -- day_suffix           VARCHAR(4) NOT NULL,
  wday_name             VARCHAR(9) NOT NULL,
  wday                  INT NOT NULL,
  date_day              INT NOT NULL,
  -- day_of_quarter       INT NOT NULL,
  -- day_of_year          INT NOT NULL,
  -- week_of_month        INT NOT NULL,
  year_week             INT NOT NULL,
  -- year_week_iso        CHAR(10) NOT NULL,
  a_month               INT NOT NULL,
  month_name            VARCHAR(9) NOT NULL,
  month_abbr            CHAR(3) NOT NULL,
  a_quarter             INT NOT NULL,
  -- quarter_name         VARCHAR(9) NOT NULL,
  a_year                INT NOT NULL,
  week_date             DATE NOT NULL,
  -- last_wday            DATE NOT NULL,
  month_date            DATE NOT NULL,
  -- last_date_day        DATE NOT NULL,
  quarter_date          DATE NOT NULL,
  -- last_day_of_quarter  DATE NOT NULL,
  -- first_day_of_year    DATE NOT NULL,
  -- last_day_of_year     DATE NOT NULL,
  mmyyyy                CHAR(6) NOT NULL,
  mmddyyyy              CHAR(10) NOT NULL,
  is_working            BOOLEAN NOT NULL
);

ALTER TABLE public.calendar ADD CONSTRAINT calendar_pk PRIMARY KEY (date_id);

CREATE INDEX calendar_idx
  ON calendar(a_date);

COMMIT;

INSERT INTO calendar
SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_id,
       datum AS a_date,
       EXTRACT(epoch FROM datum) AS epoch,
      --  TO_CHAR(datum,'fmDDth') AS day_suffix,
       TO_CHAR(datum,'Day') AS wday_name,
       EXTRACT(isodow FROM datum) AS wday,
       EXTRACT(DAY FROM datum) AS date_day,
      --  datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
      --  EXTRACT(doy FROM datum) AS day_of_year,
      --  TO_CHAR(datum,'W')::INT AS week_of_month,
       EXTRACT(week FROM datum) AS year_week,
      --  TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS year_week_iso,
       EXTRACT(MONTH FROM datum) AS a_month,
       TO_CHAR(datum,'Month') AS month_name,
       TO_CHAR(datum,'Mon') AS month_abbr,
       EXTRACT(quarter FROM datum) AS a_quarter,
      --  CASE
      --    WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
      --    WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
      --    WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
      --    WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
      --  END AS quarter_name,
       EXTRACT(isoyear FROM datum) AS a_year,
       datum +(1 -EXTRACT(isodow FROM datum))::INT AS week_date,
      --  datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_wday,
       datum +(1 -EXTRACT(DAY FROM datum))::INT AS month_date,
      --  (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_date_day,
       DATE_TRUNC('quarter',datum)::DATE AS quarter_date,
      --  (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
      --  TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
      --  TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum,'mmyyyy') AS mmyyyy,
       TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
       CASE
         WHEN EXTRACT(isodow FROM datum) = 7 THEN FALSE
         ELSE TRUE
       END AS is_working
FROM (SELECT '2016-01-01'::DATE+ SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES (0, 7305) AS SEQUENCE (DAY)   -- 7305 = 365.25 x 20
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;