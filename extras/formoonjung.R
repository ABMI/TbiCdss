library(PatientLevelPrediction)
library(TBIv1)

##connection server
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
outputFolder <- '~/output/MortalityWithLabResults'

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
                                              cohortId = 1370, outcomeIds = 776,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              outcomeDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable,
                                              outcomeTable = cohortTable,
                                              covariateSettings=covariateSettings)

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

covariateSettings <- list()
#covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
covaraiteSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = FALSE,
  useDemographicsAge = FALSE,
  useDemographicsAgeGroup = FALSE,
  useConditionOccurrenceShortTerm = FALSE,
  useVisitCountShortTerm = FALSE,
  shortTermStartDays = -3,
  endDays = 0,
  useMeasurementValueShortTerm = FALSE,
  useDrugExposureShortTerm = FALSE,
  useDrugEraShortTerm = FALSE,
  useProcedureOccurrenceShortTerm = FALSE
)

data <- getPlpData(
  connectionDetails=connectionDetails,
  cdmDatabaseSchema=cdmDatabaseSchema,
  oracleTempSchema = cdmDatabaseSchema,
  studyStartDate = 19000101,
  studyEndDate = 21991231,
  cohortId=1370,
  outcomeIds=776,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  outcomeDatabaseSchema = cohortDatabaseSchema,
  outcomeTable = cohortTable,
  cdmVersion = "5",
  firstExposureOnly = FALSE,
  washoutPeriod = 0,
  sampleSize = NULL,
  covariateSettings,
  excludeDrugsFromCovariates = FALSE
)

## connect to db
con <- dbConnect(drv=RSQLite::SQLite(), dbname=data$covariateData@dbname)
## list all tables
tables <- dbListTables(con)
#df <- dbGetQuery(conn=con, "select * from covariates")
covariateIds <- dbGetQuery(conn=con, "select * from covariateRef where conceptId in (8507,
                           8532,
                           3004249,
                           3012888,
                           3027018,
                           3024171,
                           40762499,
                           197320,
                           43008898,
                           3007461,
                           3024128,
                           3030477,
                           3013682,
                           3017250,
                           3016723,
                           3010813,
                           42920714,
                           1321341,
                           42922562,
                           1507835,
                           1337720,
                           1337860,
                           1343916,
                           79936,
                           75365,
                           9202,
                           9203,
                           9201,
                           262

)")

library(dplyr)
covariateIds <- paste0(covariateIds$covariateId, collapse = ",")
#df <- dbGetQuery(conn=con, "select * from covariates")
df <- dbGetQuery(conn=con, paste0("select * from covariates where covariateId IN (", covariateIds, ")"))
df <- df %>% left_join(dbGetQuery(conn=con, "select * from covariateRef"), by=c("covariateId"="covariateId"))

population2 <- population

for(i in 1:length(unique(df$conceptId))) {                                    # Head of for-loop
  new <- rep(0, nrow(population2))                                            # Create new column
  population2[ , ncol(population2) + 1] <- new                                # Append new column
  colnames(population2)[ncol(population2)] <- paste0("new", i)                # Rename column name
}
s
newcolnames <- c(colnames(population2)[1:13], unique(df$conceptId))
colnames(population2) <- newcolnames

for (i in 14:length(population2)) {
  temp <- df[df[,"conceptId"]==colnames(population2)[i],"rowId"]
  idx <- population2$rowId %in% temp
  population2[idx,i] <- 1
}


