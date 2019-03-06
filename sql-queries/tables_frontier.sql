--inventory
CREATE TABLE car_inventory ( 
  inventory_date  DATE         NOT NULL,
  car_id          VARCHAR (40) NOT NULL,
  selling_status  VARCHAR (32), 
  physical_status VARCHAR (32), 
  legal_status    VARCHAR (32), 
  internal_id     VARCHAR (16), 
  vehicle_id      VARCHAR (32),
  car_location    VARCHAR (32),
  car_name        VARCHAR (128), 
  car_cost        FLOAT   , 
  allowance_cost  FLOAT   ,  
  total_cost      FLOAT   ,
  incoming_date   TIMESTAMP, 
  inventory_days  INTEGER ,
  status_days     INTEGER ,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP,
  PRIMARY KEY (inventory_date, car_id)
);


-- CREATE TABLE sellers (

-- );


-- CREATE TABLE cars (

-- );


-- CREATE TABLE car_purchases (

-- );


-- CREATE TABLE car_sells (
-- );


-- CREATE TABLE car_prices (

-- );


CREATE TABLE cars_dwh (
  car_id                      VARCHAR (40) PRIMARY KEY,
  internal_car_id             VARCHAR (16), 
  car_purchased_date          DATE, 
  car_manufacturer_name       VARCHAR (20), 
  car_model_name              VARCHAR (20),   
  year_manufactured           INTEGER, 
  car_name                    VARCHAR (50), 
  car_handedover_from_seller  TIMESTAMP, 
  car_handedover_to_buyer     TIMESTAMP, 
  reserved_at_date            TIMESTAMP, 
  car_created                 TIMESTAMP, 
  car_trim                    VARCHAR (50), 
  car_mileage                 INTEGER,
  car_color                   VARCHAR (20), 
  car_vin                     VARCHAR (20),
  car_license_plate           VARCHAR (10),
  car_engine_number           VARCHAR (20),
  car_selling_status          VARCHAR (20), 
  car_physical_status         VARCHAR (20),
  car_legal_status            VARCHAR (20),
  car_purchase_payment_status VARCHAR (20),
  car_selling_payment_status  VARCHAR (20),
  car_document_status         VARCHAR (20),
  car_current_location        VARCHAR (30),
  car_purchase_location       VARCHAR (30),
  car_selling_location        VARCHAR (30),
  car_selling_status_last_modified TIMESTAMP,
  number_of_transits          INTEGER,
  car_purchase_price_car      INTEGER,
  otros_costos                INTEGER,
  car_purchase_price_other    INTEGER,
  car_purchase_price_total    INTEGER,
  car_purchase_taxless        FLOAT,
  car_purchase_price_car_taxless FLOAT,
  car_purchase_price_total_taxless FLOAT,
  car_selling_taxless         FLOAT,
  car_selling_price_car_taxless FLOAT,
  car_selling_price_total_taxless FLOAT,
  car_selling_price_car       INTEGER,
  car_selling_price_total     INTEGER,
  source_car_selling_price_total INTEGER,
  purchase_channel            VARCHAR (20),
  purchased_by                VARCHAR (30),
  sales_channel               VARCHAR (20),
  sold_by                     VARCHAR (30),
  sold_to_id                  VARCHAR (30),
  dealer_name                 VARCHAR (40),
  sold_to                     VARCHAR (30),
  invoice_date                TIMESTAMP,
  car_outgoing_payment_total  FLOAT,
  car_outgoing_payment_total_net FLOAT,
  car_incoming_payment_total  FLOAT,
  car_incoming_payment_total_net FLOAT,
  car_sold_date               DATE,
  latest_outgoing_payment_date TIMESTAMP,
  latest_incoming_payment_date TIMESTAMP,
  source_car_id               VARCHAR (40),
  client_subtype              VARCHAR (20),
  sold_delivered              BOOLEAN,
  original_sold_confirmed     BOOLEAN,
  sold_confirmed              BOOLEAN,
  was_purchased               BOOLEAN,
  inventory_days              INVENTORY,
  inventory_isin              BOOLEAN,
  initial_valuation_price     INTEGER,
  auction_class               VARCHAR (20),
  auction_last_date           TIMESTAMP,
  car_allowance               FLOAT,
  b2b_deal                    VARCHAR (30),
  deleted_at                  TIMESTAMP,
  inspection_qc_score         FLOAT,
  inspector                   VARCHAR (40)
);