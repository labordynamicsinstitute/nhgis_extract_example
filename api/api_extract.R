
## -------------------------------------------------------
#
# api_extract.R
#
# Author: Finn Roberts
#
# API client functions to aid in submitting and retrieving extracts using IPUMS'
# NHGIS API (https://developer.ipums.org/docs/get-started/).
#
# BETA VERSION
#
## -------------------------------------------------------

library(httr)
library(jsonlite)
library(ipumsr)
library(tidyverse)

## Create extract object
#
# See https://developer.ipums.org/docs/workflows/create_extracts/nhgis_data/
# for argument descriptions.
new_nhgis_extract <- function(datasets = NULL,
                              data_tables = NULL,
                              ds_geog_levels = NULL,
                              years = NULL,
                              breakdown_values = NULL,
                              time_series_tables = NULL,
                              tst_geog_levels = NULL,
                              shapefiles = NULL,
                              data_format = NULL,
                              description = NULL,
                              breakdown_and_data_type_layout = NULL,
                              #single_file = NULL,
                              time_series_table_layout = NULL,
                              geographic_extents = NULL) {
  
  extract <- list(
    datasets = datasets,
    data_tables = data_tables,
    ds_geog_levels = ds_geog_levels,
    years = years,
    breakdown_values = breakdown_values,
    time_series_tables = time_series_tables,
    tst_geog_levels = tst_geog_levels,
    shapefiles = shapefiles,
    data_format = data_format,
    description = description,
    breakdown_and_data_type_layout = breakdown_and_data_type_layout,
    #single_file = single_file,
    time_series_table_layout = time_series_table_layout,
    geographic_extents = geographic_extents
  )
  
  structure(
    extract,
    class = "nhgis_extract"
  )
  
}

## Convert a single time series table into appropriate list format for conversion to JSON
format_single_time_series_table <- function(time_series_table, geog_levels) {
  
  time_series_table_list <- list(
    time_series_table = list(
      geog_levels = geog_levels
    )
  )
  
  time_series_table_list <- map(time_series_table_list, compact)
  time_series_table_list <- setNames(time_series_table_list, time_series_table)
  
  time_series_table_list  
}

## Convert multiple time series tables into appropriate list format for conversion to JSON
format_time_series_tables <- function(time_series_tables, geog_levels) {
  
  if(length(time_series_tables) == 1) {
    
    time_series_tables_list <- format_single_time_series_table(
      time_series_table = time_series_tables,
      geog_levels = geog_levels
    )
    
    return(time_series_tables_list)
  }
  
  time_series_tables_list <- list(time_series_tables, geog_levels)
  
  time_series_tables_list <- map(
    1:length(time_series_tables), 
    ~map(time_series_tables_list, .x)
  )
  
  time_series_tables_list <- purrr::flatten(
    map(
      time_series_tables_list,
      ~format_single_time_series_table(.x[[1]], .x[[2]])
    )
  )
  
  time_series_tables_list
  
}

## Convert single dataset into appropriate list format for conversion to JSON
format_single_dataset <- function(dataset, data_tables, geog_levels, years = NULL, breakdown_values = NULL) {
  
  dataset_list <- list(
    dataset = list(
      data_tables = data_tables,
      geog_levels = geog_levels,
      years = years,
      breakdown_values = breakdown_values
    )
  )
  
  dataset_list <- map(dataset_list, compact)
  dataset_list <- setNames(dataset_list, dataset)
  
  dataset_list
  
}

## Convert multiple datasets into appropriate list format for conversion to JSON
format_datasets <- function(datasets, data_tables, geog_levels, years = NULL, breakdown_values = NULL) {
  
  if(length(datasets) == 1) {
    dataset_list <- format_single_dataset(
      dataset = datasets,
      data_tables = data_tables,
      geog_levels = geog_levels,
      years = years,
      breakdown_values = breakdown_values
    )
    
    return(dataset_list)
  }
  
  dataset_list <- list(datasets, data_tables, geog_levels, years, breakdown_values)
  
  dataset_list <- map(
    1:length(datasets), 
    ~map(dataset_list, .x)
  )
  
  dataset_list <- purrr::flatten(
    map(
      dataset_list, 
      ~format_single_dataset(.x[[1]], .x[[2]], .x[[3]], .x[[4]], .x[[5]])
    )
  )
  
  dataset_list
  
}

## Convert extract object to JSON format for API request
extract_to_request_json <- function(extract) {
  
  if (is.null(extract$description)) {
    extract$description <- ""
  }
  
  if(is.null(extract$datasets)) {
    datasets <- format_empty_request_field(extract$datasets)
  } else {
    datasets <- format_datasets(
      extract$datasets, 
      extract$data_tables, 
      extract$ds_geog_levels, 
      extract$years, 
      extract$breakdown_values
    )
  }
  
  if(is.null(extract$time_series_tables)) {
    time_series_tables <- format_empty_request_field(extract$time_series_tables)
  } else {
    time_series_tables <- format_time_series_tables(
      extract$time_series_tables,
      extract$tst_geog_levels
    )
  }
  
  extract_list <- list(
    datasets = datasets,
    time_series_tables = time_series_tables,
    shapefiles = extract$shapefiles,
    data_format = unbox(extract$data_format),
    description = unbox(extract$description),
    breakdown_and_data_type_layout = unbox(extract$breakdown_and_data_type_layout),
    #single_file = unbox(extract$single_file),
    time_series_table_layout = unbox(extract$time_series_table_layout),
    geographic_extents = extract$geographic_extents
  )
  
  extract_list <- compact(extract_list)
  
  jsonlite::toJSON(extract_list, pretty = TRUE)
  
}

## Helper function to prevent NULL entries for datasets or time series tables
## from showing up as empty elements in JSON format
format_empty_request_field <- function(request_field) {
  
  if(!is.null(names(request_field))) {
    request_field
  } else {
    NULL
  }
  
}

# nhgis_post()
#
# POST request to NHGIS API
#
# url: API url
#
# key:  IPUMS API key (obtain at https://developer.ipums.org/docs/get-started/)
#
# json: path to local file containing JSON formatted extract request
nhgis_post <- function(url, key, json) {
  
  body <- jsonlite::fromJSON(json, simplifyVector = FALSE)
  
  resp <- POST(
    url,
    add_headers(Authorization = key),
    body = body,
    encode = "json"
  )
  
  post_cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = FALSE)
  
  if(http_error(resp)) {
    stop(
      glue::glue(
        "Extract submission failed.\n\n",
        "Status: {post_cont$status$code}\n",
        "Details:\n{glue::glue_collapse(post_cont$detail, sep = '\n')}\n\n"
      ),
      call. = FALSE
    )
  }
  
  return(post_cont)
  
}

# nhgis_get_single()
#
# GET request based on previous extract POST request
#
# path: API path
#
# key:  IPUMS API key (obtain at https://developer.ipums.org/docs/get-started/)
#
# extract_number: number of extract request provided in nhgis POST response
nhgis_get_single <- function(url, key, extract_number) {
  
  # construct URL to get extract status, incorporating extract number
  url <- modify_url(url, path = paste0("extracts/", extract_number))
  
  # Repeat these lines until status is "complete"
  # request status of extract
  resp <- GET(
    url, 
    add_headers(Authorization = key)
  )
  
  get_cont <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = FALSE)
  
  if(http_error(resp)) {
    stop(
      sprintf(
        "Extract retrieval failed.\n\nStatus: [%s]\nError: %s",
        status_code(resp),
        get_cont$error
      ),
      call. = FALSE
    )
  }
  
  return(get_cont)
  
}

# nhgis_get()
#
# Repeatedly poll status of GET request until finished, then return request
#
# delay: number of seconds to wait between each check of request status
#
# ...:   additional arguments to be passed to nhgis_get_single()
nhgis_get <- function(delay = 5, ...) {
  
  req <- nhgis_get_single(...)
  
  status <- req$status
  
  if(status != "completed") {
    message(glue::glue("Trying again in {delay} seconds"))
    Sys.sleep(delay)
    nhgis_get(delay, ...)
  } else {
    return(req)
  }
  
}

# nhgis_download()
#
# Automated download of files based on API response
#
# req:        Results of GET request to API server
#
# key:        API key
#
# write_dir:  Directory to write files to. Either single string or list of 
#             same length as the number of files to be downloaded
#
# data_type:  One of "codebook_preview", "table_data", or "gis_data" that 
#             indicates which data types to download from the extract's download links
#
# fnames:     file names for writing. If NULL, file names populated based on the API response. Otherwise, must be list
#             of same length as number of files to be downloaded (length of download_links field of extract response)
#
# overwrite:  whether to overwrite files with the given file names at the write location
nhgis_download <- function(req, 
                           key, 
                           write_dir, 
                           data_type = NULL, 
                           fnames = NULL, 
                           overwrite = FALSE) {
  
  ## Manually handle default data_type
  if(is.null(data_type)) {
    data_type <- c("codebook_preview", "table_data", "gis_data")
    default_types <- TRUE
  } else {
    default_types <- FALSE
  }
  
  ## Identify invalid data types for given extract
  links_in_data_types <- intersect(data_type, names(req$download_links))
  types_not_in_links <- setdiff(data_type, names(req$download_links))
  
  if((length(data_type) != length(fnames)) && !is.null(fnames) && !default_types) {
    stop(
      glue::glue(
        "Number of data types requested ({length(data_type)}) and number of file names ({length(fnames)}) do not match."
      ),
      call. = FALSE
    )
  }
  
  ## Subset to links associated with requested data types
  links <- req$download_links[links_in_data_types]
  
  ## Remove invalid and/or irrelevant data types and associated file names
  if(length(types_not_in_links) != 0) {
    
    if(!is.null(fnames)) {
      
      if(!default_types) {
        warning(
          glue::glue(
            "\"{glue::glue_collapse(types_not_in_links, sep = '\", \"', last = '\" and \"')}\" ",
            "requested in `data_type` argument, but not available for this extract.\n",
            "These types and their associated file names",
            "(\"{glue::glue_collapse(fnames[which(data_type %in% types_not_in_links)], sep = '\", \"', last = '\" and \"')}\") ignored."
          ),
          call. = FALSE
        )
      }
      
      fnames <- fnames[which(!data_type %in% types_not_in_links)]
      
    } else if(!default_types) {
      warning(
        glue::glue(
          "`data_type` \"{glue::glue_collapse(types_not_in_links, sep = '\", \"', last = '\" and \"')}\" ",
          "not avilable for this extract and will be ignored."
        ),
        call. = FALSE
      )
      
    }
    
    ## Remove valid but irrelevant data types (not available for given extract)
    data_type <- data_type[data_type %in% links_in_data_types]
    
  }
  
  if(is.null(fnames)) {
    message(
      "No file names specified. Using default file names...\n"
    )
    
    fnames <- map(links, ~str_extract(.x, "([^/]+$)"))
  }
  
  # if(length(fnames) != length(links)) {
  #   stop(
  #     glue::glue(
  #       "Lengths of file names (length = {length(fnames)}) and download links (length = {length(links)}) must match.\n"
  #     ),
  #     call. = FALSE
  #   )
  # }
  
  ## Handle potential overwrite cases
  if(!dir.exists(write_dir)) {
    dir.create(write_dir)
  }
  
  all_files <- list.files(write_dir)
  
  file_exists <- any(fnames %in% all_files)
  existing_files <- fnames[which(fnames %in% all_files)]
  
  if(file_exists && !overwrite) {
    
    links <- links[which(setdiff(fnames, all_files) == fnames)]
    fnames <- setdiff(fnames, all_files)
    
    warning(
      glue::glue(
        "File(s) \"{glue::glue_collapse(existing_files, sep = '\", \"', last = '\" and \"')}\" already exist(s) in file write directory.\n",
        "To write file(s), please change file name or set `overwrite = TRUE`\n"
      ),
      call. = FALSE
    )
  } else if(file_exists && overwrite) {
    warning(
      glue::glue(
        "File(s) \"{glue::glue_collapse(existing_files, sep = '\", \"', last = '\" and \"')}\" ",
        "already existed in file write directory and were/was overwritten."
      ),
      call. = FALSE
    )
  }
  
  paths <- map(fnames, ~paste0(write_dir, "/", .x))
  
  walk2(
    links,
    paths,
    ~download.file(
      .x,
      destfile = .y,
      headers = c(Authorization = key)
    )
  )
  
}

# nhgis_api()
#
# Wrapper to combine POST and GET components of NHGIS API data extract request
#
# url:   API url
#
# key:   IPUMS API key (obtain at https://developer.ipums.org/docs/get-started/)
#
# json:  path to local file containing JSON formatted extract request
#
# delay: number of seconds to wait between each check when 
#        polling server for request status
nhgis_api <- function(url, key, json, delay = 5) {
  
  post_resp <- nhgis_post(url, key, json)
  
  ## GET request for extract status
  req <- nhgis_get(
    delay = delay,
    url = url, 
    key = key, 
    extract_number = post_resp$number
  )
  
  return(req)
  
}

## nhgis_api_full()
#
# Wrapper to submit and retrieve an extract in one function call
nhgis_api_full <- function(datasets = NULL,
                           data_tables = NULL,
                           ds_geog_levels = NULL,
                           years = NULL,
                           breakdown_values = NULL,
                           time_series_tables = NULL,
                           tst_geog_levels = NULL,
                           shapefiles = NULL,
                           data_format = NULL,
                           description = NULL,
                           breakdown_and_data_type_layout = NULL,
                           time_series_table_layout = NULL,
                           geographic_extents = NULL,
                           key = Sys.getenv("IPUMS_API_KEY"),
                           write_dir = getwd(),
                           url = "https://api.ipums.org/extracts/?product=nhgis&version=v1",
                           delay = 5,
                           ...) {
  
  extract <- new_nhgis_extract(
    datasets = datasets,
    data_tables = data_tables,
    ds_geog_levels = ds_geog_levels,
    years = years,
    breakdown_values = breakdown_values,
    time_series_tables = time_series_tables,
    tst_geog_levels = tst_geog_levels,
    shapefiles = shapefiles,
    data_format = data_format,
    description = description,
    breakdown_and_data_type_layout = breakdown_and_data_type_layout,
    time_series_table_layout = time_series_table_layout,
    geographic_extents = geographic_extents
  )
  
  ## Convert to JSON
  extract_json <- extract_to_request_json(extract)
  
  ## Submit extract to API
  extract_response <- nhgis_api(
    url = url,
    key = key,
    json = extract_json,
    delay = delay
  )
  
  # ## Download files from completed extract
  nhgis_download(
    extract_response,
    key = key,
    write_dir = write_dir,
    ...
  )
}

