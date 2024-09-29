library(tidyverse)
library(readxl)
library(DBI)

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
                        "resource/{resource}/",
                        "download/{data_file}"))

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

  t <- basename(url)
  print(t)
  if (!file.exists(t)) download.file(url, t)
  pq_name <-
    str_c(
      str_replace(basename(url), "datadotgov_(.*)\\.(csv|xlsx)$", "\\1"),
      ".parquet")

  pq_dir <- file.path(Sys.getenv("DATA_DIR"), "acnc")
  if (!dir.exists(pq_dir)) dir.create(pq_dir, recursive = TRUE)

  pq_path <- file.path(pq_dir, pq_name)

  if (file_ext(url) == "xlsx") {
   df_raw <- read_xlsx(t, na = "N/A", guess_max = 10000)
   } else if (file_ext(url) == "csv") {
    df_raw <- read_csv(t,
                       col_types = cols(`fundraising number - act` =
                                          col_character()),
                       locale = locale(date_format = "%d/%m/%Y"))
   }

  db <- dbConnect(duckdb::duckdb())
  df_db <- copy_to(db, df = df_raw, overwrite = TRUE)

  if (any(str_detect(colnames(df_db), "Reporting Obligations"))) {
    reporting_obligs <-
      df_db |>
      rename_with(str_to_lower, any_of("ABN")) |>
      select(abn, matches("Reporting Obligations")) |>
      pivot_longer(cols = -abn,
                   names_to = "report_oblig") |>
      filter(value == "y") |>
      mutate(report_oblig = str_replace(report_oblig, "^.*\\s+", "")) |>
      mutate(report_oblig = str_to_upper(report_oblig)) |>
      group_by(abn) |>
      summarize(report_oblig = array_agg(report_oblig),
                .groups = "drop") |>
      compute()
  } else {
    reporting_obligs <- tbl(db, sql("SELECT NULL AS abn"))
  }

  if (any(str_detect(colnames(df_db), "submitted report"))) {
    report_submissions <-
      df_db |>
      rename_with(str_to_lower, any_of("ABN")) |>
      select(abn, matches("submitted report ")) |>
      pivot_longer(cols = -abn,
                   names_to = "submitted_report") |>
      filter(value == "y") |>
      mutate(submitted_report = str_replace(submitted_report, "^.*\\s+", "")) |>
      mutate(submitted_report = str_to_upper(submitted_report)) |>
      group_by(abn) |>
      summarize(submitted_reports = array_agg(submitted_report),
                .groups = "drop") |>
      compute()
  } else {
    report_submissions <- tbl(db, sql("SELECT NULL AS abn"))
  }

  if (any(str_detect(colnames(df_db), "association number"))) {
    assoc_nums <-
      df_db |>
      select(abn, matches("association number ")) |>
      pivot_longer(cols = -abn,
                   names_to = "state",
                   values_to = "assoc_num") |>
      filter(!is.na(assoc_num)) |>
      mutate(state = str_replace(state, "^.*\\s+", "")) |>
      mutate(state = str_to_upper(state)) |>
      compute() |>
      distinct() |>
      group_by(abn) |>
      summarize(states = array_agg(state),
                assoc_nums = array_agg(assoc_num),
                .groups = "drop") |>
      mutate(assoc_nums = map(states, assoc_nums)) |>
      select(-states) |>
      compute()
  } else {
    assoc_nums <- tbl(db, sql("SELECT NULL AS abn"))
  }

  if (any(str_detect(colnames(df_db), "fundraising -"))) {
    fundraising <-
      df_db |>
      select(abn, matches("fundraising -")) |>
      pivot_longer(cols = -abn,
                   names_to = "state",
                   values_to = "fundraising") |>
      filter(fundraising == "y") |>
      mutate(state = str_replace(state, "^.*\\s+", "")) |>
      mutate(state = str_to_upper(state)) |>
      compute() |>
      group_by(abn) |>
      summarize(fundraising = array_agg(state),
                .groups = "drop") |>
      compute()
  } else {
    fundraising <- tbl(db, sql("SELECT NULL AS abn"))
  }

  if (any(str_detect(colnames(df_db), "fundraising number -"))) {
    fundraising_nums <-
      df_db |>
      select(abn, matches("fundraising number -")) |>
      compute() |>
      pivot_longer(cols = -abn,
                   names_to = "state",
                   values_to = "fundraising_num") |>
      compute() |>
      filter(!is.na(fundraising_num)) |>
      mutate(state = str_replace(state, "^.*\\s+", "")) |>
      mutate(state = str_to_upper(state)) |>
      compute() |>
      distinct() |>
      group_by(abn) |>
      summarize(states = array_agg(state),
                fundraising_nums = array_agg(fundraising_num),
                .groups = "drop") |>
      compute() |>
      mutate(fundraising_nums = map(states, fundraising_nums)) |>
      select(-states) |>
      compute()
  } else {
    fundraising_nums <- tbl(db, sql("SELECT NULL AS abn"))
  }


  df_main <-
    df_raw |>
    select(-matches("submitted report "),
           -matches("association number "),
           -matches("fundraising -"),
           -matches("fundraising number -")) |>
    fix_names() |>
    mutate(source_file = basename(url),
           across((matches("^date") | matches("_date") |
                     matches("^fin_report_(from|to)")) & !where(is.Date), dmy),
           charity_size = str_to_sentence(charity_size),
           across(where(is_yn), yn_to_bool),
           type_of_financial_statement =
             if_else(str_detect(type_of_financial_statement, "simplified"),
                     "General purpose, simplified", type_of_financial_statement)) |>
    copy_to(db, df = _, name = "df_main", overwrite = TRUE)

  df_final <-
    df_main |>
    left_join(report_submissions, by = join_by(abn)) |>
    left_join(assoc_nums, by = join_by(abn)) |>
    left_join(fundraising, by = join_by(abn)) |>
    left_join(fundraising_nums, by = join_by(abn)) |>
    compute(name = "df_final", overwrite = TRUE)

  res <- dbExecute(db, str_c("COPY df_final TO '", pq_path, "'"))
  dbDisconnect(db)
  res
}

map(urls$url, get_data)

ais <- copy_to(db, df2)


report_submissions
  ais |>
  select(abn, matches("submitted_report_")) |>
  pivot_longer(matches("submitted_report_")) |>


acnc_main_states <-
  acnc_main |>


df1 <- map(urls$url[1], get_data)[[1]]
df2 <- map(urls$url[2], get_data)[[1]]

setdiff(colnames(df2), colnames(df1))
setdiff(colnames(df1), colnames(df2))
