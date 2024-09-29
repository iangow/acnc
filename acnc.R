library(tidyverse)
library(arrow)
library(farr)

registry <-
  read_parquet('https://go.unimelb.edu.au/5d78') |>
  collect() |>
  system_time()

beneficiaries <-
  registry |>
  unnest(beneficiaries) |>
  rename(beneficiary = beneficiaries) |>
  select(abn, beneficiary) |>
  distinct()

subtypes <-
  registry |>
  unnest(subtypes) |>
  rename(subtype = subtypes) |>
  select(abn, subtype) |>
  distinct()

beneficiaries |>
  inner_join(beneficiaries, by = "abn",
             relationship = "many-to-many") |>
  filter(beneficiary.x < beneficiary.y) |>
  count(beneficiary.x, beneficiary.y) |>
  arrange(desc(n))

registry |>
  unnest(operating_countries) |>
  select(abn, operating_countries) |>
  filter(operating_countries != "AUS") |>
  count(operating_countries, sort = TRUE) |>
  system_time()

registry |>
  unnest(operating_countries) |>
  select(abn, operating_countries) |>
  filter(operating_countries != "AUS") |>
  group_by(abn) |>
  summarize(num_countries = n(),
            .groups = "drop") |>
  count(num_countries, sort = TRUE) |>
  system_time()

registry |>
  unnest(subtypes) |>
  count(subtypes, sort = TRUE) |>
  system_time()

registry |>
  unnest(beneficiaries) |>
  count(beneficiaries, sort = TRUE) |>
  print(n = Inf) |>
  system_time()

