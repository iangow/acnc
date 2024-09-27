library(tidyverse)

urls <- tribble(
  ~year, ~dataset, ~resource, ~data_file,
  2022L, "311c24f3-fc09-42e7-8362-f15a76334a75", "cfbcf6f1-7ce5-472f-bfd3-a478e67e0366", "datadotgov_ais22.csv",
  2021L, "db8def1b-b3ba-4536-98eb-60c99e5495e5", "bbb19fa5-2f63-49cb-96b2-523e25828f27", "datadotgov_ais21.csv",
  2020L, "28a94374-d516-4200-a6f8-c09e76f58cc5", "62c7b3d3-4358-4d57-b810-a36732d36e2d", "datadotgov_ais20.csv",
  2019L, "34b35c52-8af0-4cc1-aa0b-2278f6416d09", "76006467-18f2-4094-a1cb-50544fd9b7b2", "datadotgov_ais19.csv",
  2018L, "cfc1a18e-f4e0-4ed8-9a19-36b59b7a3d5b", "9312452f-fced-476e-a6ec-2b2327796a34", "datadotgov_ais18.xlsx",
  2017L, "a1f8626c-fefb-4c4d-86ea-deaa04fb1f6e", "8d020b50-700f-4bc4-8c78-79f83d99be7a", "datadotgov_ais17.xlsx",
  2016L, "7e073d71-4eef-4f0c-921b-9880fb59b206", "b4a08924-af4f-4def-96f7-bf32ada7ee2b", "datadotgov_ais16.xlsx",
  2015L, "86cad799-2601-4f23-b02c-c4c0fc3b6aff", "569b8e48-a0ad-4008-9d95-7f91b6cfa2aa", "datadotgov_ais15.xlsx",
  2014L, "d7992845-5d3b-4868-b012-71f672085412", "4d65259d-1ccf-4c78-a223-e2bd49dc5fb1", "datadotgov_ais14.xlsx",
  # 2013L, "cc9d8524-39d8-4374-84b9-20e9d1070e82", "ce8bf129-9525-4353-a747-d89d8d4b5cc6", "datadotgov_ais13.xlsx"
) |>
  mutate(url = str_glue("https://data.gov.au/data/",
                        "dataset/{dataset}/",
                        "resource/{resource}/", "
                        download/{data_file}"))

fix_names <- function(df) {
  colnames(df) <- str_replace_all(colnames(df), "[-/\\s]+", "_")
  colnames(df) <- str_to_lower(colnames(df))
  df
}

yn_to_bool <- function(x) {
  case_when(x == "y" | x == "Yes" ~ TRUE,
            x == "n" | x == "No" ~ FALSE,
            .default = NA)
}

is_yn <- function(x) {
  all(x %in% c("y", "n", NA)) | all(x %in% c("Yes", "No", NA))
}

file_ext <- function(filename) {
  str_extract(filename, "[a-zA-Z0-9]*?$")
}

get_data <- function(url) {

  t <- tempfile()
  download.file(url, t)

  pq_name <-
    str_c(
      str_replace(basename(url), "datadotgov_(.*)\\.(csv|xlsx)$", "\\1"),
      ".parquet")

  pq_dir <- file.path(Sys.getenv("DATA_DIR"), "acnc")
  if (!dir.exists(pq_dir)) dir.create(pq_dir, recursive = TRUE)

  pq_path <- file.path(pq_dir, pq_name)

  if (file_ext(url) == "xlsx") {
   df_raw <- readxl::read_xlsx(t) }
  else if (file_ext(url) == "csv") {
    df_raw <- read_csv(t,
                       col_types = cols(`fundraising number - act` =
                                          col_character()),
                       locale = locale(date_format = "%d/%m/%Y"))
    }

  df_raw |>
    fix_names() |>
    mutate(source_file = basename(url),
           across((matches("^date") | matches("_date") |
                     matches("^fin_report_(from|to)")) & !where(is.Date), dmy),
           charity_size = str_to_sentence(charity_size),
           across(where(is_yn), yn_to_bool),
           type_of_financial_statement =
             if_else(str_detect(type_of_financial_statement, "simplified"),
                     "General purpose, simplified", type_of_financial_statement)) |>
    arrow::write_parquet(pq_path)
}

map(urls$url, get_data)

ais |> select(matches("(date|^fin_report)"))
ais |> count(source_file) |> arrange(source_file)

revocations <-
  ais |>
  mutate(year = str_c("20",
                      str_replace(source_file, "datadotgov_ais([0-9]+).(xlsx|csv)", "\\1")),
         status =
           case_when(str_detect(registration_status, "Voluntarily") ~ "Voluntarily revoked",
                     str_detect(registration_status, "Revoked") ~ "Revoked",
                     registration_status == "Registered" ~ "Registered",
                     .default = registration_status)) |>
  filter(registration_status != "Registered") |>
  mutate(abn = sql("abn::BIGINT")) |>
  group_by(abn, status) |>
  summarize(year = min(year, na.rm = TRUE),
            .groups = "drop") |>
  compute()


revocations |> count(status, year) |> arrange(desc(year))


datadotgov_ais22 |> count(conducted_activities)
datadotgov_ais22 |> count(charity_size)

colnames(datadotgov_ais22)
problems(datadotgov_ais22)
