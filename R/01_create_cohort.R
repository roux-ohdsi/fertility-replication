# Packages
library(DatabaseConnector)
library(CohortGenerator)
library(ROhdsiWebApi)

# ============================================================================

# Credentials
usr <- keyring::key_get("lab_user")
pw <- keyring::key_get("lab_password")

# DB Connections
base_url <- "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
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

# ============================================================================

authorizeWebApi(
  base_url,
  authMethod = "db",
  webApiUsername = usr,
  webApiPassword = pw
)

cohort_definition <- exportCohortDefinitionSet(
  baseUrl = base_url,
  cohortIds = 1289
)

# choose what the table names should be called that hold the cohort and
# statistics about it
cohort_table_names <- getCohortTableNames(cohortTable = "fertility_replication")

# create empty tables in the {mySchema}.cohort table
createCohortTables(
  connection = con,
  cohortTableNames = cohort_table_names,
  cohortDatabaseSchema = my_schema
)

# find people matching cohort definition
cohortsGenerated <- generateCohortSet(
  connection = con,
  cdmDatabaseSchema = cdm_schema,
  cohortDatabaseSchema = my_schema,
  cohortTableNames = cohort_table_names,
  cohortDefinitionSet = cohort_definition
)

disconnect(con)
