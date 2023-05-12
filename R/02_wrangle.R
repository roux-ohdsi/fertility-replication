# Packages
library(tidyverse)
library(DatabaseConnector)
library(ROhdsiWebApi)
library(ohdsilab)

# ==== setup ===================================================================

# Credentials
usr <- keyring::key_get("lab_user")
pw <- keyring::key_get("lab_password")

# DB Connections
atlas_url <- "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema <- "omop_cdm_53_pmtx_202203"
my_schema <- paste0("work_", usr)

# Create the connection
con <- connect(
  dbms = "redshift",
  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
  port = 5439,
  user = usr,
  password = pw
)

options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)
options(atlas_url.default.value = atlas_url)

authorizeWebApi(
  atlas_url,
  authMethod = "db",
  webApiUsername = usr,
  webApiPassword = pw
)

# ===== index events ===========================================================

my_cohort <- "fertility_replication"

cohort <- tbl(con, paste(my_schema, my_cohort, sep = ".")) |>
  select(person_id = subject_id, cohort_start_date, cohort_end_date) |>
  mutate(
    covariate_start_date = dateAdd("year", -1, cohort_start_date),
    txt_end_date = dateAdd("month", 4, cohort_start_date)
  )

ART <- pull_concept_set(cohort,
  concept_set_id = 1125, concept_set_name = "ART",
  start_date = cohort_start_date,
  end_date = txt_end_date, keep_all = TRUE
)

IUI <- pull_concept_set(cohort,
  concept_set_id = 1124, concept_set_name = "IUI",
  start_date = cohort_start_date,
  end_date = txt_end_date, keep_all = TRUE
)

# ===== covariates =============================================================

tobacco <- pull_concept_set(cohort,
  concept_set_id = 1157, concept_set_name = "tobacco",
  start_date = covariate_start_date, end_date = cohort_start_date
)
PCOS <- pull_concept_set(cohort,
  concept_set_id = 1982, concept_set_name = "PCOS",
  start_date = covariate_start_date, end_date = cohort_start_date
)
alcohol <- pull_concept_set(cohort,
  concept_set_id = 1190, concept_set_name = "alcohol",
  start_date = covariate_start_date, end_date = cohort_start_date
)
obesity <- pull_concept_set(cohort,
  concept_set_id = 1126, concept_set_name = "obesity",
  start_date = covariate_start_date, end_date = cohort_start_date
)
female_infertility <- pull_concept_set(cohort,
  concept_set_id = 2279, concept_set_name = "f_infert",
  start_date = covariate_start_date, end_date = cohort_start_date
)
male_infertility <- pull_concept_set(cohort,
  concept_set_id = 2312, concept_set_name = "m_infert",
  start_date = covariate_start_date, end_date = cohort_start_date
)

baseline <- cohort |>
  collect()

baseline <- reduce(
  list(
    baseline, alcohol, tobacco, obesity, PCOS,
    female_infertility, male_infertility
  ),
  left_join,
  by = join_by("person_id")
) |>
  mutate(across(-all_of(names(baseline)), ~ replace_na(.x, 0))) |>
  left_join(IUI, by = join_by(person_id, cohort_start_date == date), multiple = "any") |>
  left_join(ART,
    by = join_by(person_id, cohort_start_date == date), multiple = "any",
    suffix = c("_IUI", "_ART")
  ) |>
  mutate(first_txt = case_when(
    is.na(concept_id_ART) ~ "IUI",
    is.na(concept_id_IUI) ~ "ART",
    TRUE ~ "both"
  ))

filter(baseline, first_txt == "both")

# ===== pregnancies =============================================================

pregnant <- pull_concept_set(cohort,
  concept_set_id = 332, concept_set_name = "pregnant",
  start_date = cohort_start_date,
  end_date = cohort_end_date, keep_all = TRUE
)
stillbirth <- pull_concept_set(cohort,
  concept_set_id = 398, concept_set_name = "stillbirth",
  start_date = cohort_start_date,
  end_date = cohort_end_date, keep_all = TRUE
)
abortion <- pull_concept_set(cohort,
  concept_set_id = 399, concept_set_name = "abortion",
  start_date = cohort_start_date,
  end_date = cohort_end_date, keep_all = TRUE
)
labor <- pull_concept_set(cohort,
  concept_set_id = 365, concept_set_name = "labor",
  start_date = cohort_start_date,
  end_date = cohort_end_date, keep_all = TRUE
)

pregnancies <- bind_rows(pregnant, abortion, stillbirth, labor) |>
  arrange(date)

count(pregnancies, person_id, concept_set)
pregnancies |> filter(person_id == 6583393)

all_txt <- bind_rows(ART, IUI)
  
txt_dates <- all_txt |>
  arrange(date) |>
  group_by(person_id) |>
  mutate(
    day_1 = first(date),
    day_14 = day_1 + days(14),
    in_cycle_1 = between(date, day_1, day_14)
  ) |>
  ungroup()

txt_dates |>
  filter(in_cycle_1) |>
  group_by(person_id) |>
  count(concept_set) |>
  filter(n() > 1)

# both, apparently...
all_txt |>
  filter(person_id == 152938900)

later_txt <- txt_dates |>
  filter(!in_cycle_1) |>
  arrange(date) |>
  group_by(person_id) |>
  mutate(
    day_1b = first(date),
    day_14b = day_1b + days(14),
    in_cycle_1b = between(date, day_1b, day_14b)
  ) |>
  ungroup()

quickly_after <- later_txt |>
  group_by(person_id) |>
  slice(1) |>
  filter(day_1b - day_1 < 21)

txt_dates |>
  filter(person_id %in% quickly_after$person_id) |>
  arrange(date) |>
  group_by(person_id) |>
  slice(1) |>
  bind_rows(quickly_after) |>
  group_by(person_id) |>
  summarise(max(date) - min(date))

txt_dates |> filter(person_id == 32381128)

four_cycles <- txt_dates |>
  group_by(person_id, in_cycle_1) |>
  arrange(date) |>
  mutate(
    day_1 = if_else(!in_cycle_1, first(date), day_1),
    day_14 = if_else(!in_cycle_1, day_1 + days(14), day_14),
    in_cycle_2 = if_else(!in_cycle_1, between(date, day_1, day_14), FALSE)
  ) |>
  group_by(person_id, in_cycle_1, in_cycle_2) |>
  arrange(date) |>
  mutate(
    day_1 = if_else(!in_cycle_1 & !in_cycle_2, first(date), day_1),
    day_14 = if_else(!in_cycle_1 & !in_cycle_2, day_1 + days(14), day_14),
    in_cycle_3 = if_else(!in_cycle_1 & !in_cycle_2, between(date, day_1, day_14), FALSE)
  ) |>
  group_by(person_id, in_cycle_1, in_cycle_2, in_cycle_3) |>
  arrange(date) |>
  mutate(
    day_1 = if_else(!in_cycle_1 & !in_cycle_2 & !in_cycle_3, first(date), day_1),
    day_14 = if_else(!in_cycle_1 & !in_cycle_2 & !in_cycle_3, day_1 + days(14), day_14),
    in_cycle_4 = if_else(!in_cycle_1 & !in_cycle_2 & !in_cycle_3, between(date, day_1, day_14), FALSE)
  ) |>
  ungroup() |>
  filter(in_cycle_1 | in_cycle_2 | in_cycle_3 | in_cycle_4)


# use coalesce with selecting functions
new_coalesce <- function(.cols, ...) {
  df <- across({{ .cols }}, .fns = identity)
  classes <- sapply(df, class)
  if (length(unique(classes)) > 1) df <- mutate(df, across(everything(), as.character))
  df <- mutate(df, .res = coalesce(!!!syms(names(df))))
  pull(df, .res)
}

# check for multiple treatments within the same cycle
# add back in code for cancellation as no pregnancy

cycle_outcomes <- four_cycles |>
  mutate(across(starts_with("in_"), ~ ifelse(.x, str_remove(cur_column(), "in_cycle_"), NA)),
    cycle = new_coalesce(starts_with("in_")),
    cycle = parse_number(cycle)
  ) |>
  group_by(person_id, cycle) |>
  slice(1) |>
  group_by(person_id) |>
  arrange(cycle) |>
  mutate(next_cycle = lead(day_1)) |>
  ungroup() |>
  select(person_id, concept_set, day_1, cycle, next_cycle) |>
  mutate(end_preg = if_else(
    is.na(next_cycle), day_1 + days(45 * 7), next_cycle
  )) |>
  left_join(
    select(pregnancies,
      person_id, date,
      preg_concept = concept_set
    ),
    by = join_by(person_id, between(y$date, x$day_1, x$end_preg))
  ) |>
  group_by(person_id, cycle, preg_concept) |>
  arrange(date) |>
  slice(1) |>
  ungroup() |>
  pivot_wider(names_from = preg_concept, values_from = date) |>
  left_join(select(baseline, person_id, cohort_end_date), by = join_by(person_id)) |>
  mutate(outcome = case_when(
    !is.na(abortion) & abortion < day_1 + days(20 * 7) ~ "pregnancy loss",
    !is.na(stillbirth) ~ "pregnancy loss",
    !is.na(labor) ~ "live_birth",
    cohort_end_date < end_preg ~ "censored",
    !is.na(pregnant) ~ "pregnancy",
    .default = "no pregnancy"
  )) |>
  select(person_id,
    txt = concept_set, day_1, cycle, next_cycle, abortion,
    stillbirth, pregnant, labor, outcome
  )

cycle_outcomes

ITT_data <- cycle_outcomes |>
  group_by(person_id) |>
  summarise(
    any_pregnancy = as.numeric(any(outcome == "pregnancy") | any(outcome == "live_birth")),
    any_live_birth = as.numeric(any(outcome == "live_birth")),
    delivery_date = min(labor, na.rm = TRUE),
    pregnancy_date = min(pregnant, na.rm = TRUE),
    last_cycle = max(day_1),
    censored = as.numeric(all(outcome == "censored"))
  ) |>
  full_join(baseline, by = join_by(person_id)) |>
  mutate(across(c(delivery_date, pregnancy_date), ~ if_else(is.infinite(.x), NA_Date_, .x)),
    event_pregnancy = !is.na(pregnancy_date),
    event_delivery = !is.na(delivery_date),
    pregnancy_date = if_else(event_pregnancy & pregnancy_date < cohort_start_date + days(424),
      pregnancy_date,
      pmin(cohort_end_date, cohort_start_date + days(424))
    ), # 14 months post-randomization
    delivery_date = if_else(event_delivery & delivery_date < cohort_start_date + days(424),
      delivery_date,
      pmin(cohort_end_date, cohort_start_date + days(424))
    ),
    time_to_pregnancy = time_length(interval(cohort_start_date, pregnancy_date), "days"),
    time_to_delivery = time_length(interval(cohort_start_date, delivery_date), "days"),
    IUI = factor(first_txt == "IUI", labels = c("ART", "IUI"))
  ) # for adjustedsurv function

# some people clearly have messed up data
filter(four_cycles, person_id == 2142694)
filter(pregnancies, person_id == 2142694)
filter(ITT_data, person_id == 2142694) |> select(contains("event"), contains("time"))

# remove 20 rows... ok
ITT_data_analysis <- ITT_data |>
  filter((!event_pregnancy | time_to_pregnancy > 0) & (!event_delivery | time_to_delivery > 0))

pr_IUI <- glm(IUI ~ alcohol + tobacco + obesity + pcos + f_infert + m_infert,
  data = ITT_data_analysis, family = binomial()
)
# anything crazy indicating non-positivity?
summary(pr_IUI)

ITT_data_analysis$p_IUI <- predict(pr_IUI, newdata = ITT_data_analysis, type = "response")
ITT_data_analysis$ip_weight <- ifelse(ITT_data_analysis$first_txt == "IUI",
  1 / ITT_data_analysis$p_IUI,
  1 / (1 - ITT_data_analysis$p_IUI)
)
# reasonable weights
summary(ITT_data_analysis$ip_weight)

weighted <- WeightIt::weightit(IUI ~ alcohol + tobacco + obesity + pcos + f_infert + m_infert,
  data = ITT_data_analysis, method = "glm"
)
weighted
summary(weighted)
cobalt::bal.plot(weighted)
cobalt::bal.plot(weighted, var = "f_infert")
cobalt::bal.tab(weighted)
cobalt::love.plot(weighted)

adjsurv <- adjustedCurves::adjustedsurv(
  data = ITT_data_analysis,
  variable = "IUI",
  ev_time = "time_to_delivery",
  event = "event_delivery",
  method = "iptw_km",
  treatment_model = IUI ~ alcohol + tobacco + obesity + pcos + f_infert + m_infert,
  weight_method = "glm",
  conf_int = TRUE,
  bootstrap = FALSE
)
adjsurv$weights |> head()
weighted$weights |> head()
ITT_data_analysis$ip_weight |> head()

plot(adjsurv, ylim = c(0, 1))

adjsurv$adjsurv |>
  group_by(group) |>
  filter(time == max(time))

adjustedCurves::adjusted_curve_diff(adjsurv, conf_int = TRUE) |>
  ggplot(aes(time, diff)) +
  geom_line() +
  geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper))
