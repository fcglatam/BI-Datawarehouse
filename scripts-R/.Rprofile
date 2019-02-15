######## PARAMETROS LOCALES #############3

############## PAQUETES ################
library(tidyverse, quietly = TRUE)
library(readxl,    quietly = TRUE)
library(magrittr,  quietly = TRUE)
library(lubridate, quietly = TRUE)
library(stringr,   quietly = TRUE)
library(rlang, quietly = TRUE)
# Se utiliza paquete odbc
  
extract <- magrittr::extract
filter  <- dplyr::filter

################ Algunas funciones útiles #################################
file_at    <- function (file.name, ...) file.path(..., file.name)
not_na     <- function (x) not(is.na(x))
na_true    <- function (x) not(not(x) %in% TRUE)
cumsum_rev <- function (x) rev(cumsum(rev(x)))
  

equal_and_nas <- function (x, y) {
  are_equal_ <- (x == y) | (is.na(x) & is.na(y))
  are_equal  <- are_equal_ %in% TRUE
  return (are_equal)
}

classes <- function (a_frame) {
  data_classes <- data_frame(
    name = names(a_frame), 
    class = sapply(a_frame, 
        . %>% class() %>% extract(1)) )
  return (data_classes)
}



# Verificiar DRIVER de ODBC
conn_params <- list(
  host   = Sys.getenv("DB_HOST"),
  dbname = Sys.getenv("DB_NAME"),
  user   = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"))   


dbConnect_pg <- function () {
  require(RPostgreSQL)
  conn_pg <- conn_params %$% DBI::dbConnect(RPostgreSQL::PostgreSQL(), 
    host = host,
    dbname = dbname,
    user = user,
    password = password)
  
  return (conn_pg)
}

if (interactive()) try (fortunes::fortune(), silent=TRUE)









