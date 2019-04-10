CREATE TABLE ga_quotes ( 
  quote_id        VARCHAR (32),
  quote_date      DATE NOT NULL,
  make            VARCHAR (100),
  model           VARCHAR (100), 
  car_year        INTEGER, 
  car_trim        VARCHAR (256), 
  kilometrage     INTEGER, 
  car_quote       INTEGER,
  n_quotes        INTEGER, 
  PRIMARY KEY (quote_id)
);




