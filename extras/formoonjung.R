library(PatientLevelPrediction)
library(TBIv1)
library(dplyr)
library(glmnet)

##connection server
# add details of your database setting:
# databaseName <- 'add a shareable name for the database you are currently validating on'

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
                                              cohortId = 1370, outcomeIds = 776, # cohortId = 1373,
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
covariateSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = T,
  useDemographicsAge = T,
  useDemographicsAgeGroup = T,
  useConditionOccurrenceShortTerm = T,
  useVisitCountShortTerm = T,
  shortTermStartDays = -3,
  endDays = 0,
  useMeasurementValueShortTerm = T,
  useDrugExposureShortTerm = T,
  useDrugEraShortTerm = T,
  useDistinctIngredientCountShortTerm = T, #
  useProcedureOccurrenceShortTerm = T
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
## list all tables
tables <- dbListTables(con)
#covlist <- dbGetQuery(conn=con, "select * from analysisRef")
#df <- dbGetQuery(conn=con, "select * from covariates")
#tempdf <- dbGetQuery(conn=con, "select * from covariates where covariateId = '8507001' or '8532001' or  ")
covariateIds <- dbGetQuery(conn=con, "select * from covariateRef where conceptId in (
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


covariateIds <- paste0(covariateIds$covariateId, collapse = ",")
#df <- dbGetQuery(conn=con, "select * from covariates limit 10;")
df <- dbGetQuery(conn=con, paste0("select * from covariates where covariateId IN (", covariateIds, ")"))
df <- df %>% left_join(dbGetQuery(conn=con, "select * from covariateRef"), by=c("covariateId"="covariateId"))
df <- df %>% filter()

population2 <- population

for(i in 1:length(unique(df$conceptId))) {                                    # Head of for-loop
  new <- rep(0, nrow(population2))                                            # Create new column
  population2[ , ncol(population2) + 1] <- new                                # Append new column
  colnames(population2)[ncol(population2)] <- paste0("new", i)                # Rename column name
}

newcolnames <- c(colnames(population2)[1:13], unique(df$conceptId))
colnames(population2) <- newcolnames

for (i in 14:length(population2)) {
  temp <- df[df[,"conceptId"]==colnames(population2)[i],"rowId"]
  idx <- population2$rowId %in% temp
  population2[idx,i] <- 1
}

df <- population2
df <- df[,c(8:10, 14,15, 18:length(df))]
df$gender <- ifelse(df$gender=='8532', 0, 1)

label = as.integer(df$outcomeCount)
# table(label)
# label
# 0     1
# 56388  7765

n <- nrow(df)
# ncol(df)
# [1] 19
# df %>% colSums()

train.index = sample(n,floor(0.75*n))
train.data = df[train.index,]
# train.data = df[train.index,c(1,2, 4:ncol(df))]
train.label = label[train.index]
# table(train.label)

test.data = df[-train.index,]
# test.data = df[-train.index,c(1,2, 4:ncol(df))]
# test.data = as.matrix(df[-train.index,c(1,2)])
test.label = label[-train.index]
# table(test.label)

# 2. L1 regression from glmnet
lambdas_to_try <- 10^seq(-3, 5, length.out = 100)
# Setting alpha = 1 implements lasso regression
# lasso_cv <- cv.glmnet(as.matrix(train.data),
#                       as.factor(train.label),
#                       family="binomial",
#                       alpha = 1,
#                       lambda = lambdas_to_try,
#                       type.measure = "class",
#                       standardize = TRUE,
#                       nfolds = 3)

model <- glm(outcomeCount ~., data = train.data, family = 'binomial')
summary(model)
model$coefficients

prob <- model %>% predict(test.data, type='response')

# plot(lasso_cv)
# lambda_cv <- lasso_cv$lambda.min
# # Fit final model, get its sum of squared residuals and multiple R-squared
# model_cv <- glmnet(as.matrix(train.data),
#                    as.factor(train.label),
#                    alpha = 1,
#                    lambda = lambda_cv,
#                    type.measure = "class",
#                    family = "binomial",
#                    standardize = TRUE,
#                    nfolds = 3)
# # plot(model_cv)
# # model_cv <- glmnet(train.data, train.label, alpha = 1, lambda = lambdas_to_try, standardize = TRUE, family = "binomial")
# pred <- predict(model_cv, s = lambda_cv, newx = as.matrix(test.data), type="class")
# table(test.label, pred)
# #plot(pred)
# coef(lasso_cv,s=lambda_cv)
#
# lasso_assess <- assess.glmnet(model_cv, newx = as.matrix(test.data), newy = test.label, family="binomial")
# lasso_matrix <- confusion.glmnet(model_cv, newx = as.matrix(test.data), newy = test.label, family="binomial")
# lasso_auc <- roc.glmnet(model_cv, newx = as.matrix(test.data), newy = test.label, family="binomial")
