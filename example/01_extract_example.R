
## --------------------------------------------------------
#
# 01_extract_example.R
#
# Author: Finn Roberts
# Date:   2021-01-06
#
# Example code to specify, submit, download, and open data for all tables
# for a single dataset retrieved via the NHGIS API.
#
# Downloaded extracts are saved to the /extracts/01_extract/ directory within this
# project directory.
#
# This example uses beta functionality for the NHGIS API R client that
# will later be added to the ipumsr package (https://www.github.com/ipums/ipumsr).
#
# API documentation can be found at https://developer.ipums.org/docs/workflows/create_extracts/nhgis_data/
#
## --------------------------------------------------------

## Setup --------------------------------------------------

# Import local API R client functions
source(here::here("api", "api_extract.R"))
source(here::here("api", "api_metadata.R"))

# API Key
# Either initialize key directly or add key to R environment with usethis::edit_r_environ()
key <- Sys.getenv("IPUMS_API_KEY")

## Generate extract request specs -------------------------

# See listing of all datasets
# get_nhgis_metadata("datasets", key)

# Example dataset
dataset <- "1940_tPH_Major"

# Identify all tables for given dataset
ds_meta <- get_dataset_metadata(dataset, key)
data_tables <- ds_meta$data_tables$name

# Specify geographic level of interest
# ds_meta$geog_levels # See available geog_levels for given dataset
geog_levels <- "tract"

## Submit requests and download response data ------------

nhgis_api_full(
  datasets = dataset,
  data_tables = data_tables,
  ds_geog_levels = geog_levels,
  data_format = "csv_no_header",
  write_dir = here::here("extracts", "01_example"), # Save downloaded extracts to /extracts dir within this project
  fnames = paste0("nhgis_", dataset, ".zip"),
  data_type = "table_data",
  overwrite = FALSE
)

## Open downloaded data ----------------------------------

nhgis_data <- ipumsr::read_nhgis(
    here::here("extracts", "01_example", paste0("nhgis_", dataset, ".zip")) # Update path to be consistent with write_dir above
)
