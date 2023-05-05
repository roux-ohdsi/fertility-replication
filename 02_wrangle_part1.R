
# Packages
library(keyring)
library(DatabaseConnector)
library(FeatureExtraction)
library(tidyverse)
library(ohdsilab)
library(aouFI)
# ============================================================================


# Credentials

usr = keyring::key_get("lab_user")
pw  = keyring::key_get("lab_password")

# DB Connections

base_url = "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema = "omop_cdm_53_pmtx_202203"
my_schema = paste0("work_", keyring::key_get("lab_user"))

# Create the connection
con =  DatabaseConnector::connect(dbms = "redshift",
                                  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
                                  port = 5439,
                                  user = keyring::key_get("lab_user"),
                                  password = keyring::key_get("lab_password"))

class(con)

# ============================================================================

# experimental functions to help with querying

options(con.default.value = con)
options(schema.default.value = cdm_schema)

# ============================================================================

code_list = read_rds(file = "data/omop_codes.rds")





