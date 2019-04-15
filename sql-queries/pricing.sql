CREATE TABLE pricing_cars (

);



CREATE TABLE pricing_kavak (
  id            VARCHAR(50),

  stock_id      INTEGER,
  make          VARCHAR(50), 
  model         VARCHAR(50), 
  car_year      INTEGER, 
  kilometer     INTEGER, 
  sell_price    INTEGER, 

  download_time   TIMESTAMP,  
  stock_time      TIMESTAMP, 
  stock_status    VARCHAR(50),
  stock_location  VARCHAR(50),
  
  vehicle_id    VARCHAR(20),
  car_link      VARCHAR(200),
  car_type      VARCHAR(50), 
  engine        VARCHAR(50),
  savings       INTEGER ,
  
  has_warranty    VARCHAR(20),  
  color_exterior  VARCHAR(50), 
  color_interior  VARCHAR(50),
  doors           INTEGER,
  passengers      INTEGER,
  fuel_type       VARCHAR(50),
  traction        VARCHAR(50),
  transmission    VARCHAR(50),
  highway_consumption   VARCHAR(50),
  city_consumption      VARCHAR(50),
  combined_consumption  VARCHAR(50),
  horsepower      INTEGER,
  keys            INTEGER, 

  PRIMARY KEY (id)
);



CREATE TABLE pricing_autometrica (


); 