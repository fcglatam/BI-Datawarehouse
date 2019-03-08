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


CREATE TABLE x_dwh_cars (
  car_id                      VARCHAR (36) PRIMARY KEY,
  internal_car_id             VARCHAR (16), 
  car_purchased_date          DATE, 
  car_manufacturer_name       VARCHAR (64), 
  car_model_name              VARCHAR (64),   
  year_manufactured           INTEGER, 
  car_name                    VARCHAR (128), 
  car_handedover_from_seller  TIMESTAMP, 
  car_handedover_to_buyer     TIMESTAMP, 
  reserved_at_date            TIMESTAMP, 
  original_reserved_at_date   TIMESTAMP,
  car_created                 TIMESTAMP, 
  car_trim                    VARCHAR (256), 
  car_fuel                    VARCHAR (256),
  car_mileage                 INTEGER,
  car_transmission            VARCHAR (256),
  car_color                   VARCHAR (256), 
  car_vin                     VARCHAR (256),
  car_license_plate           VARCHAR (20),
  car_engine_number           VARCHAR (20),
  car_selling_status          VARCHAR (256), 
  car_physical_status         VARCHAR (256),
  car_legal_status            VARCHAR (256),
  car_purchase_payment_status VARCHAR (256),
  car_selling_payment_status  VARCHAR (256),
  car_document_status         VARCHAR (256),
  car_current_location        VARCHAR (256),
  car_purchase_location       VARCHAR (256),
  car_selling_location        VARCHAR (256),
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
  purchase_channel            VARCHAR (256),
  purchased_by                VARCHAR (256),
  sales_channel               VARCHAR (256),
  sold_by                     VARCHAR (256),
  sold_to_id                  VARCHAR (256),
  dealer_name                 VARCHAR (256),
  dealer_tax_id               VARCHAR (256),
  dealer_company_name         VARCHAR (256),
  sold_to                     VARCHAR (256),
  invoice_date                TIMESTAMP,
  car_outgoing_payment_total  FLOAT,
  car_outgoing_payment_total_net FLOAT,
  car_incoming_payment_total  FLOAT,
  car_incoming_payment_total_net FLOAT,
  car_sold_date               DATE,
  latest_outgoing_payment_date TIMESTAMP,
  latest_incoming_payment_date TIMESTAMP,
  source_car_id               VARCHAR (256),
  client_subtype              VARCHAR (256),
  sold_delivered              INTEGER,
  original_sold_confirmed     INTEGER,
  sold_confirmed              INTEGER,
  was_purchased               INTEGER,
  inventory_days              INTEGER,
  inventory_isin              INTEGER,
  initial_valuation_price     INTEGER,
  auction_class               VARCHAR (256),
  auction_last_date           TIMESTAMP,
  car_allowance               FLOAT,
  b2b_deal                    VARCHAR (256),
  deleted_at                  TIMESTAMP,
  inspection_qc_score         FLOAT,
  inspector                   VARCHAR (256)
);