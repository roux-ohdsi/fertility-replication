
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

# OMOP codes

# ======================== Baseline Coariates

# for OMOP concepts from ICD - assumption is th check descendents
# check descendents to get initial list, remove the ones we dont using phoebe
# then use that list as the final list of codes - hand off to louisa - she will
# check again using pheobe to get final list

# Ascertain Window: "Enrollment up to 1st treatment procedure"

# from phenotype library: #32, obesity https://data.ohdsi.org/PhenotypeLibrary/
omop_overweight_obesity                = c(4215968, 4176962, 3025315, 4099154, 3013762, 3027492, 3023166) # also exclude this one: 4264825
# OMOP: Tobacco dependence syndrome
omop_tobacco_use_disorder              = c(437264) # 4103418 is tobacco dependence syndrome in remission - exclude? 37109024 is chewing
# From Alcohol use disorder in Phenotype library. This defiition seems fairly broad, but its a bit hard to pick out the alcohol concepts for dependence/abuse
omop_alcohol_abuse_or_dependence       = c(36714559, 4042860, 435140, 46269816, 37311131, 4058714, 46269817, 195300, 318773, 37016176, 45757783, 44788725, 40484946, 3661461, 44788726, 4261832, 46269818, 4083236)
omop_male_infertility                  = c(198197) # this one is interesting. would you also want to include male + infertility? (4311387)
omop_female_infertility                = c(201909) # again - concerned we might miss some infertility codes. can we do infertility + m/f?
omop_female_infertility_unspecified    = c() # this seems like more of an error in their table setup...
# the icd9 maps to 36683296 which is just "polycystic ovary and there's a subcode for polycystic ovary syndrome and polycystic right ovary and polycystic left ovary. should we include the 
# syndrome OMOP code, or should we include the code for polycystic ovary (36683296) which includes all 3?
omop_polycystic_ovarian_syndrome       = c(40443308) 

# ======================== Neonatal outcomes

# Ascertain Window: "From Treatment to Delivery"

omop_multiple_gestation                        =  c(432969) # this one looks pretty good but we should check for an lower-order exclusions

# Ascertain Window: "Maternal and infant claims within 30 days of birth"

omop_small_for_gestational_age                 =  c(72693, 4145947) # 4145947 small for gest. age has higher concept fetal growth restriction - might apply?
omop_large_for_gestational_age                 =  c(434758, 440532, 81636) # first code is exceptionaly large baby. 4079859 is high birth weight. do we want to include?
omop_neonatal_icu_admission                    =  c(42739633,42739550, 42739551, 2514563, 2514564, 2514565, 2514566, 2514569, 2514570, 2514571, 2514572) # CPT codes easy to match
omop_neonatal_icu_admission_infant_record      =  c(2108681, 2108682, 2514441, 2514442) # CPT codes easy to match. these are only if found in the infant record though...

# ======================== Maternal outcomes

# Need to meet all of conditions below to establish gestational DM
# see individual code for ascertain window

# build in R or custom covariate builder in feature extraction

# At least one of these for GDM
omop_gdm_condition_1 = c(2212361, 2212362, 4162865) # 4162865 is for 648.8 - needs manual review to make sure its sufficient/not too broad
# At least one code for DM after 140 gestational days and the delivery. (side note...logical statement is confusing)

omop_gdm_condition_2 = c(201820) # ICD9CM 250.xx - any codes for DM at all. This is just the omop code for general DM then. 

# an absence of DM codes for GDM, DM complicating pregnancy, or DM generally prior to 140 gestational days. 
omop_gdm_condition_2 = c(4162865, 76751001, 201820) # 648.0x maps to 76751001 "Diabetes mellitus in mother complicating pregnancy, childbirth AND/OR puerperium (disorder)"
# but I wonder if we should go one step up in the hierarchy: 4058243 "Diabetes mellitus during pregnancy, childbirth and the puerperium (disorder)"

# Ascertain Window: 140+ gestational days and within 30 days after delivery 

omop_preeclampsia    = c(439393) # must occur 2x in the inpatient record. 624.4x-642.7x ICD codes - maybe best to just go with the general concept from OMOP? needs manual review.
omop_gestational_htn = c(441922) # This is the direct mapping from ICD9 - but looks like it might be good to look up the hierarchy. review. 
# Note: "Women who met the criteria of both preeclampsia and gestational hypertension were classified as preeclampsia"

# ======================== To Discuss

# How to ID 140 gestational days?
# Phenotype library for Pregnancy?

# ======================== Next Steps: 

# Map  CPT codes pertaining to ART or ART in Supplemental Table 2. 

cpt_tx <- 
  c("58321", "58322", "S4017", "S4020", "S4021", "89254", "58970",
    "76948", "S4042", "89290", "89291", "S4015", "S4011", "89342",
    "89255", "58974", "89250", "89251", "89268", "89272", "89354",
    "89356", "S4037", "89352", "89258", "S4016", "S4018", "S4022",
    "89280", "89281")

omop_tx_codes <- map2omop(con, cdm_schema, codes = cpt_tx, translate_from = c("CPT4", "HCPCS")) |> 
  mutate(treatment = c("IUI", "IUI", rep("ART", (length(cpt_tx)-2))))


code_list = mget(ls(pattern = "omop_"))

write_rds(code_list, file = "data/omop_codes.rds")




