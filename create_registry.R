library(tidyverse)
library(DBI)

dataset <- "b050b242-4487-4306-abf5-07ca073e5594"
resource <- "8fb32972-24e9-4c95-885e-7140be51be8a"
file_name <- "datadotgov_main.csv"

url <- str_glue("https://data.gov.au/data/",
                "dataset/{dataset}/",
                "resource/{resource}/",
                "download/{file_name}")

t <- tempfile()
download.file(url, t)

df_raw <- read_csv(t, locale = locale(date_format = "%d/%m/%Y"))

fix_names <- function(x) {
  x <- str_replace_all(x, "[-/\\s]+", "_")
  x <- str_to_lower(x)
  x
}

db <- dbConnect(duckdb::duckdb())

acnc_main <- copy_to(db, df_raw)

acnc_main_states <-
  acnc_main |>
  select(ABN, matches("Operates_in")) |>
  pivot_longer(matches("Operates_in")) |>
  filter(value == "Y") |>
  rename(abn = ABN) |>
  mutate(state = str_replace(name, "Operates_in_", "")) |>
  group_by(abn) |>
  summarize(states = array_agg(state),
            .groups = "drop") |>
  compute()

acnc_main_countries <-
  acnc_main |>
  filter(!is.na(Operating_Countries)) |>
  select(ABN, Operating_Countries) |>
  rename(abn = ABN,
         operating_countries = Operating_Countries) |>
  mutate(operating_countries =
           regexp_split_to_array(operating_countries, ",\\s*")) |>
  compute()

anc_subtypes <-
  acnc_main |>
  rename(abn = ABN) |>
  select(abn,
         PBI:Advancing_security_or_safety_of_Australia_or_Australian_public) |>
  pivot_longer(-abn, names_to = "subtype") |>
  filter(value == "Y") |>
  mutate(subtype = str_replace_all(subtype, "_", " ")) |>
  group_by(abn) |>
  summarize(subtypes = array_agg(subtype)) |>
  compute()

acnc_beneficiaries <-
  acnc_main |>
  rename(abn = ABN) |>
  select(abn,
         Aboriginal_or_TSI:Youth) |>
  pivot_longer(-abn, names_to = "beneficiary") |>
  filter(value == "Y") |>
  mutate(beneficiary = str_replace_all(beneficiary, "_", " ")) |>
  group_by(abn) |>
  summarize(beneficiaries = array_agg(beneficiary)) |>
  compute()

acnc_registry <-
  acnc_main |>
  select(-matches("^operat"),
         -(PBI:Advancing_security_or_safety_of_Australia_or_Australian_public),
         -(Aboriginal_or_TSI:Youth)) |>
  rename_with(fix_names) |>
  left_join(acnc_beneficiaries, by = join_by(abn)) |>
  left_join(anc_subtypes, by = join_by(abn)) |>
  left_join(acnc_main_countries, by = join_by(abn)) |>
  left_join(acnc_main_states, by = join_by(abn)) |>
  compute(name = "registry")

pq_dir <- file.path(Sys.getenv("DATA_DIR"), "acnc")
if (!dir.exists(pq_dir)) dir.create(pq_dir, recursive = TRUE)

pq_name <- "registry.parquet"

pq_path <- file.path(pq_dir, pq_name)

dbExecute(db, str_c("COPY registry TO '", pq_path, "'"))

dbDisconnect(db)
