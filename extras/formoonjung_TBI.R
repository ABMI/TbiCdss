library(PatientLevelPrediction)
library(TBIv1)
library(dplyr)
library(glmnet)
# install.packages("imputeTS")
# library(tidyr)

#connection server
# add details of your database setting:
databaseName <- 'add a shareable name for the database you are currently validating on'

# add the cdm database schema with the data
cdmDatabaseSchema <- 'your cdm database schema for the validation'

# add the work database schema this requires read/write privileges
cohortDatabaseSchema <- 'your work database schema'

# if using oracle please set the location of your temp schema
oracleTempSchema <- NULL

# the name of the table that will be created in cohortDatabaseSchema to hold the cohorts
cohortTable <- 'MortalityWithLabResults'

# the location to save the prediction models results to:
outputFolder <- '~/MortalityWithLabResults'

# add connection details:
options(fftempdir = 'T:/fftemp')
dbms <- "pdw"
user <- NULL
pw <- NULL
server <- Sys.getenv('server')
port <- Sys.getenv('port')
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)


#######learning code
ParallelLogger::logInfo("Creating Cohorts")
createCohorts(connectionDetails,
              cdmDatabaseSchema=cdmDatabaseSchema,
              cohortDatabaseSchema=cohortDatabaseSchema,
              cohortTable=cohortTable,
              outputFolder = outputFolder)

##Create population
# outcome 776
# 1372 JMPark_Hospitalized_TBI_patients
# 1370 JMPark_Hospitalized_patients_with_intensive_care
# 1373 JMPark_Hospitalized_TBI_patients_with_intensive_care

covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = T,
                                                                useDemographicsAge = T,
                                                                useDemographicsAgeGroup = T)

plpData <- PatientLevelPrediction::getPlpData(connectionDetails,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              cohortId = 1372, outcomeIds = 776, # cohortId = 1373,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              outcomeDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable,
                                              outcomeTable = cohortTable,
                                              covariateSettings=covariateSettings)
# outcome data
population <- PatientLevelPrediction::createStudyPopulation(plpData = plpData,
                                                            outcomeId = 776,
                                                            binary = T,
                                                            includeAllOutcomes = T,
                                                            requireTimeAtRisk = T,
                                                            minTimeAtRisk = 1,
                                                            riskWindowStart = 1,
                                                            riskWindowEnd = 30,
                                                            removeSubjectsWithPriorOutcome = T)

connection <- DatabaseConnector::connect(connectionDetails)

# feature/covariate extraction
covariateSettings <- list()
covariateSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = T,
  useDemographicsAge = T,
  useDemographicsAgeGroup = T,
  useConditionOccurrenceShortTerm = T,
  # useVisitCountShortTerm = T,
  shortTermStartDays = -7,
  endDays = 1,
  useMeasurementValueShortTerm = T,
  useDrugExposureShortTerm = T,
  useDrugEraShortTerm = T,
  useDrugGroupEraOverlapping = T, #
  useDistinctIngredientCountShortTerm = T, #
  useProcedureOccurrenceShortTerm = T,
  useDeviceExposureShortTerm = T, #
  addDescendantsToInclude = T, #
)

data <- FeatureExtraction::getDbCovariateData(
  connectionDetails = connectionDetails,
  #connection = connection,
  oracleTempSchema = NULL,
  cdmDatabaseSchema=cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = cohortTable,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableIsTemp = FALSE,
  cohortId = -1,
  rowIdField = "subject_id",
  covariateSettings=covariateSettings,
  aggregated = FALSE
)

## connect to db
con <- dbConnect(drv=RSQLite::SQLite(), dbname=data@dbname)
ref <- dbGetQuery(conn=con, "select * from covariateRef")
# cov <- dbGetQuery(conn=con, "select * from covariates")

## list all tables
tables <- dbListTables(con)
#covlist <- dbGetQuery(conn=con, "select * from analysisRef")
#df <- dbGetQuery(conn=con, "select * from covariates")
#tempdf <- dbGetQuery(conn=con, "select * from covariates where covariateId = '8507001' or '8532001' or  ")

# ICU covariates
covariateIds <- dbGetQuery(conn=con, "select * from covariateRef where conceptId in (
3004249,
3028737,
3012888,
3027018,
3024171,
40762499,
3010813,
3026361,
3027484,
3023314,
3003338,
3007461,
3017354,
3002030,
3019069,
3022096,
3015183,
3033658,
3006906,
3035995,
3030260,
3013682,
46235680,
3027114,
3007070,
3028437,
3024561,
3024128,
3030477,
4163872,
2211327,
2211328,
1143374,
1343916,
42922555,
42919983,
36280969,
42922393,
1321341,
42920714,
19011871,
19065473,
41359871,
42966538,
42799676,
42799677,
42918966
)")

covariateIds <- paste0(covariateIds$covariateId, collapse = ",")
# continuousCovariateIds <- paste0(continuousCovariateIds$covariateId, collapse = ",")

df <- dbGetQuery(conn=con, paste0("select * from covariates where covariateId IN (", covariateIds, ")"))
df <- df %>% left_join(dbGetQuery(conn=con, "select * from covariateRef"), by=c("covariateId"="covariateId"))
df <- df %>% filter()

population2 <- population

### preprocessing
# test$covariateValue <- as.double(df$covariateValue)
value <- reshape2::dcast(df, rowId ~ conceptId, value.var = 'covariateValue', fun.aggregate = mean, na.rm=T)
# value <- value %>% select(c("3004249", "3012888", "3027018", "3024171", "40762499", "43008898", "3007461", "3024128", "3030477", "3013682", "3017250", "3016723", "3010813"))
meas <- list(3004249,
             3028737,
             3012888,
             3027018,
             3024171,
             40762499,
             3010813,
             3026361,
             3027484,
             3023314,
             3003338,
             3007461,
             3017354,
             3002030,
             3019069,
             3022096,
             3015183,
             3033658,
             3006906,
             3035995,
             3030260,
             3013682,
             46235680,
             3027114,
             3007070,
             3028437,
             3024561,
             3024128,
             3030477
)
for(i in 2:ncol(value)){
  if(colnames(value)[i] %in% meas){
    value[,i] <- imputeTS::na_mean(value[,i])
  }
}
value <- replace(value, is.na(value), 0)
temp <- left_join(population2, value, by=c("subjectId"="rowId"))
temp2 <- tidyr::drop_na(temp[,c(1,14:length(temp))])
temp3 <- inner_join(temp[,1:13], temp2, by=c("rowId"="rowId"))
df <- temp3
###
df <- df[,c(8:10,14:length(df))]
df$gender <- ifelse(df$gender=='8532', 0, 1) # female 0, male 1

label = as.integer(df$outcomeCount)
# table(label)
n <- nrow(df)

train.index = sample(n,floor(0.75*n))
train.data = df[train.index,]
train.label = label[train.index]
# table(train.label)
test.data = df[-train.index,]
test.label = label[-train.index]
# table(test.label)

model <- glm(outcomeCount ~., data = train.data, family = 'binomial')
summary(model)
model$coefficients

prob <- model %>% predict(test.data, type='response')
