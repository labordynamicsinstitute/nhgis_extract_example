
## -------------------------------------------------------
#
# api_metadata.R
#
# Author: Finn Roberts
#
# API client functions to aid in submitting and retrieving extracts using IPUMS'
# NHGIS API (https://developer.ipums.org/docs/get-started/).
#
# Initial development guided by code written for IPUMS microdata API,
# which can be found at github.com/mnpopcenter/ipumsr.
#
# These functions focus on metadata api functionality.
#
## -------------------------------------------------------

library(httr)
library(jsonlite)
library(ipumsr)
library(tidyverse)

## Metadata for all datasets, time series tables, or shapefiles
get_nhgis_metadata <- function(type = c("datasets", "time_series_tables", "shapefiles"), key) {
  
  type <- match.arg(type)
  
  url <- modify_url(
    "https://api.ipums.org/metadata/nhgis/?version=v1", 
    path = paste0("metadata/nhgis/", type)
  )
  
  resp <- GET(url, add_headers(Authorization = key))
  cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = TRUE)
  
  if(http_error(resp)) {
    stop(
      glue::glue("Extract submission failed for {url}.\n",
                 "Status: {status_code(resp)}\n",
                 "Error: {cont$detail}"),
      call. = FALSE
    )
  }
  
  tibble(cont)
  
}

## Metadata for a single dataset
get_dataset_metadata <- function(dataset, key) {
  
  url <- modify_url(
    "https://api.ipums.org/metadata/nhgis/?version=v1", 
    path = paste0("/metadata/nhgis/datasets/", dataset)
  )
  
  resp <- GET(url, add_headers(Authorization = key))
  cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = TRUE)
  
  if(http_error(resp)) {
    stop(
      glue::glue("Extract submission failed for {url}.\n",
                 "Status: {status_code(resp)}\n",
                 "Error: {cont$detail}"),
      call. = FALSE
    )
  }
  
  cont
  
}

get_table_metadata <- function(dataset, table, key) {
  
  url <- modify_url(
    "https://api.ipums.org/metadata/nhgis/?version=v1", 
    path = paste0("/metadata/nhgis/datasets/", dataset, "/data_tables/", table)
  )
  
  resp <- GET(url, add_headers(Authorization = key))
  cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = TRUE)
  
  if(http_error(resp)) {
    stop(
      glue::glue("Extract submission failed for {url}.\n",
                 "Status: {status_code(resp)}\n",
                 "Error: {cont$detail}"),
      call. = FALSE
    )
  }
  
  cont
  
}


## Metadata for a single time series table
get_tst_metadata <- function(time_series_table, key) {
  
  url <- modify_url(
    "https://api.ipums.org/metadata/nhgis/?version=v1", 
    path = paste0("/metadata/nhgis/time_series_tables/", time_series_table)
  )
  
  resp <- GET(url, add_headers(Authorization = key))
  cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = TRUE)
  
  if(http_error(resp)) {
    stop(
      glue::glue("Extract submission failed for {url}.\n",
                 "Status: {status_code(resp)}\n",
                 "Error: {cont$detail}"),
      call. = FALSE
    )
  }
  
  cont
  
}

## Metadata for shapefiles
get_shp_metadata <- function(key) {
  
  url <- "https://api.ipums.org/metadata/nhgis/shapefiles?version=v1"
  
  resp <- GET(url, add_headers(Authorization = key))
  cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = TRUE)
  
  if(http_error(resp)) {
    stop(
      glue::glue("Extract submission failed for {url}.\n",
                 "Status: {status_code(resp)}\n",
                 "Error: {cont$detail}"),
      call. = FALSE
    )
  }
  
  as_tibble(cont)
    
}







