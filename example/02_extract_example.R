
## --------------------------------------------------------
#
# 02_extract_example.R
#
# Author: Finn Roberts
# Date:   2021-01-06
#
# Example code to specify, submit, download, and open data for all tables
# for all datasets with tract-level data between 1940 and 1960. Data
# retrieved via the NHGIS API.
#
# Downloaded extracts are saved to the /extracts/02_extract/ directory within this
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

# Helper function to determine which datasets are available at tract level
check_tract <- function(dataset, key) {
  geog_levels <- get_dataset_metadata(dataset, key)$geog_levels$name
  geog_levels[str_detect(geog_levels, "tract")]
}

## Generate extract request specs -------------------------

# Identify datasets from 1940-1960 based on NHGIS metadata
all_ds <- get_nhgis_metadata("datasets", key) %>%
  filter(str_detect(group, "1940|1950|1960")) %>%
  pull(name)

# Identify tract-level geog name for all tract-level datasets
has_tract <- map(all_ds, ~check_tract(.x, key))

tract_codes <- unlist(compact(has_tract))
tract_ds <- unlist(all_ds[map_lgl(has_tract, ~length(.x) > 0)])

# Identify all tables for each tract-level dataset and organize
all_tables <- map(tract_ds, ~get_dataset_metadata(.x, key)$data_tables$name) %>%
  setNames(tract_ds) %>%
  enframe(name = "dataset", value = "tables") %>%
  mutate(geog_level = tract_codes)

## Submit requests and download response data ------------

# Iterate through all datasets. Generate and download extracts
pwalk(
  all_tables, 
  ~nhgis_api_full(
    datasets = ..1,
    data_tables = ..2,
    ds_geog_levels = ..3,
    data_format = "csv_no_header",
    write_dir = here::here("extracts", "02_example"),
    fnames = paste0("nhgis_", ..1, ".zip"),
    data_type = "table_data",
    overwrite = FALSE
  )
)

## Open downloaded data ----------------------------------

nhgis_data <- map(
  all_tables$dataset, 
  ~ipumsr::read_nhgis(
    here::here("extracts", "02_example", paste0("nhgis_", .x, ".zip"))
  )
)
