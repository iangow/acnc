library(tidyverse)
library(duckdb)

db <- dbConnect(duckdb())

registry <- tbl_file(db, "~/Dropbox/pq_data/acnc/registry.parquet")

registry |>
  mutate(operating_country = unnest(operating_countries)) |>
  select(abn, operating_country) |>
  filter(operating_country != "AUS") |>
  count(operating_country, sort = TRUE)

registry |>
  mutate(operating_country = unnest(operating_countries)) |>
  filter(operating_country != "AUS") |>
  group_by(abn) |>
  summarize(num_countries = count(operating_country),
            .groups = "drop") |>
  count(num_countries, sort = TRUE)

registry |>
  mutate(subtype = unnest(subtypes)) |>
  count(subtype, sort = TRUE)

registry |>
  mutate(beneficiary = unnest(beneficiaries)) |>
  count(beneficiary, sort = TRUE) |>
  print(n = Inf)

dbDisconnect(db)
