"""
Notes
1. No comments
a. need to know context at least so I can try to understand each section of code
2. What is the file structure for operations r files?
  a. one file for all reports/queries using comments to comment out code?
  b. multiple files for multiple reports/queries types?
  c. is there a table of contents?
  3. Access to K drive - what is share name? who grants access?
  a. K:/DMA/CORE_DB_METHODS/UtilityFunctions.r
b. K:/DMA/CORE_DB_METHODS/fridge/raw 2020-06-23.Rdata - what is the origin?
  c. K:/DMA/CORE_DB_METHODS/pass/HBT_2021-03-09.db - what is the origin? do I need SQL db access? or do I request db backup copies?
  4. At first glance
a. code is a data cleaning operation
b. from sql db to csv
c. uses modules dplyr (select, filter…), reshape2(wideorder…)
d. use r dataframes
"""

setwd('K:/DMA/HFVC')
rm(list = ls()) # clean R environment

source("K:/DMA/CORE_DB_METHODS/UtilityFunctions.r") # read utility file

# load('K:/DMA/CORE_DB_METHODS/fridge/raw 2020-06-23.Rdata') # load data
HFVC_ID <- read.csv("HFVCPatientList_330.csv", stringsAsFactors = FALSE) # read csv

datadate = '2021-03-09'

## DATABASE ##
sql_db <- "K:/DMA/CORE_DB_METHODS/pass/HBT_2021-03-09.db" # assign db title
con <- RSQLite::dbConnect(RSQLite::SQLite(), sql_db) # make connection to sql db
tbl_list <- RSQLite::dbListTables(con) # get list of db tables
for (tbl in tbl_list) { # loop through tables
  assign(tbl, RSQLite::dbReadTable(con, tbl)) # read sql table + save to tbl var
}
RSQLite::dbDisconnect(con) # close connection to sql db

require(dplyr)
library(reshape2)

S_DATA <- select(M_DATA, -unique_id, -patient_id, -linked_ids, -stophf_arm,
                 -patient_type_history, -patient_status, -followup_date, -followup_days,
                 #-grep('HeartFailure', names(M_DATA), value = TRUE),
                 -grep('EMHFGAP', names(M_DATA), value = TRUE))

# HFVC_ID$FLAG <- ifelse(HFVC_ID$id %in% M_DATA$id, 1, 0)
S_DATA$patient_type[S_DATA$id %in% HFVC_ID$id] <- "HFVC"
S_DATA <- filter(S_DATA, patient_type %in% c('HFVC','New Diagnostic','Programme','Referral'))

# length(M_DATA$id[M_DATA$id %in% HFVC_ID$id])
# HFVC_ID$id[(HFVC_ID$id %in% M_DATA$id)==FALSE]

S_DATA$HeartFailureDate_n1 <- as.Date(S_DATA$HeartFailureDate_n1,"1970-01-01")
S_DATA$HeartFailureDate_n2 <- as.Date(S_DATA$HeartFailureDate_n2,"1970-01-01")
S_DATA$HeartFailureDate_n3 <- as.Date(S_DATA$HeartFailureDate_n3,"1970-01-01")
S_DATA$min_clinic_date <- as.Date(S_DATA$min_clinic_date, '1970-01-01')
S_DATA$max_clinic_date <- as.Date(S_DATA$max_clinic_date, '1970-01-01')

# max(S_DATA$min_clinic_date[S_DATA$patient_type=='HFVC'], na.rm = TRUE)
# min(S_DATA$min_clinic_date[S_DATA$patient_type=='HFVC'], na.rm = TRUE)

# PAT_1 <- filter(S_DATA, patient_type %in% c('New Diagnostic','Programme','Referral') &
#                         min_clinic_date>=as.Date('2015-01-01') & min_clinic_date<=as.Date('2019-12-31'))
# PAT_2 <- filter(S_DATA, patient_type=='HFVC' &
#                         min_clinic_date>=as.Date('2015-01-01') & min_clinic_date<=as.Date('2019-12-31'))

S_DATA <- filter(S_DATA, min_clinic_date>=as.Date('2015-01-01') & min_clinic_date<=as.Date('2019-12-31'))

# baseline and follow up dates
# ECH$Ech_date <- as.Date(ECH$Ech_date, '1970-01-01')
# 
# ECH_n1 <- tapply.df(ECH, "Ech_date", "patient_id", "min")
# ECH_n1 <- renamevar(ECH_n1, c("Ech_date_min"), c("min_echo_date"))
# ECH_n1$min_echo_date <- as.Date(ECH_n1$min_echo_date, "1970-01-01")
# 
# ECH_nn <- tapply.df(ECH, "Ech_date", "patient_id", "max")
# ECH_nn <- renamevar(ECH_nn, c("Ech_date_max"), c("max_echo_date"))
# ECH_nn$max_echo_date <- as.Date(ECH_nn$max_echo_date, "1970-01-01")
# 
# ECH_n <- table.df(ECH, "patient_id")
# ECH_n <- renamevar(ECH_n, c("count"), c("n_echos"))
# 
# S_DATA <- merge(S_DATA, ECH_n1, by.x = "id", by.y = "patient_id", all.x = TRUE)
# S_DATA <- merge(S_DATA, ECH_nn, by.x = "id", by.y = "patient_id", all.x = TRUE)
# S_DATA <- merge(S_DATA, ECH_n, by.x = "id", by.y = "patient_id", all.x = TRUE)

# stop  hf patients
# S_DATA <- filter(S_DATA, patient_type=="STOP HF" & n_echos>=2)
# PAT_ID <- unique(S_DATA$id)

#DGN[grep("^Cardiac Failure", DGN$medical), ]
# clinic in wide format
CLN$Date <- as.Date(CLN$Date, "1970-01-01")
CLN_W <- unique(select(filter(CLN, patient_id %in% S_DATA$id), patient_id))

clinic_list <- c("Height","Weight","BMI","HR","SBP","DBP")

for (varname in clinic_list) {
  #varname='rhythm'
  TEMP1 = unique(subset(CLN, !is.na(get(varname)) & !is.na(Date),
                        c(patient_id, get(varname), Date)))
  #TEMP1$Date = TEMP1$Ech_date
  #TEMP1[,varname] = as.numeric(TEMP1[,varname])
  TEMP2 = collapse(TEMP1, c('patient_id','Date'), 'median')
  TEMP3 = renamevar(TEMP2, c('Date'), c(paste(varname,'Date',sep='')))
  TEMP4 = wideorder(TEMP3, c(varname,paste(varname,'Date',sep='')), paste(varname,'Date',sep=''), 'patient_id')
  CLN_W = merge(CLN_W, TEMP4, by='patient_id', all.x=TRUE)
}

# smoking
SND$date <- as.Date(SND$date, "1970-01-01")
SND$smoking_status[SND$smoking_status %in% c("yes","Yes")] <- "Yes"
SND$smoking_status[SND$smoking_status %in% c("no","No")] <- "No"
SND$smoking_status[SND$smoking_status %in% c("ex","Ex")] <- "Ex"

for (varname in c("smoking_status")) {
  #varname='rhythm'
  TEMP1 = unique(subset(SND, !is.na(get(varname)) & !is.na(date),
                        c(patient_id, get(varname), date)))
  #TEMP1$Date = TEMP1$Ech_date
  #TEMP1[,varname] = as.numeric(TEMP1[,varname])
  TEMP2 = collapse(TEMP1, c('patient_id','date'), 'median')
  TEMP3 = renamevar(TEMP2, c('date'), c(paste(varname,'Date',sep='')))
  TEMP4 = wideorder(TEMP3, c(varname,paste(varname,'Date',sep='')), paste(varname,'Date',sep=''), 'patient_id')
  CLN_W = merge(CLN_W, TEMP4, by='patient_id', all.x=TRUE)
}

# echo in wide format
ECH <- filter(ECH, patient_id %in% S_DATA$id)
ECH$Ech_date <- as.Date(ECH$Ech_date, '1970-01-01')
ECH <- merge(ECH,
             select(S_DATA, id, male),
             by.x='patient_id', by.y='id', all.x=TRUE)

ECH$EF_FLAG <- NA
ECH$EF_FLAG[ECH$EF< 50] <- 1
ECH$EF_FLAG[ECH$EF>=50] <- 0

ECH$EEprime_FLAG <- NA
ECH$EEprime_FLAG[ECH$EEprime_ratio> 13] <- 1
ECH$EEprime_FLAG[ECH$EEprime_ratio<=13] <- 0

ECH$LAVI_FLAG <- NA
ECH$LAVI_FLAG[ECH$LAVI> 34] <-1
ECH$LAVI_FLAG[ECH$LAVI<=34] <-0

ECH$LVMI_FLAG <- NA
ECH$LVMI_FLAG[ECH$LVMI > 115 & ECH$male==1] <- 1
ECH$LVMI_FLAG[ECH$LVMI<= 115 & ECH$male==1] <- 0
ECH$LVMI_FLAG[ECH$LVMI > 95  & ECH$male==0] <- 1
ECH$LVMI_FLAG[ECH$LVMI<= 95  & ECH$male==0] <- 0

ECH$StageB_FLAG <- ifelse(is.na(ECH$EF_FLAG) &
                            is.na(ECH$EEprime_FLAG) &
                            is.na(ECH$LAVI_FLAG) &
                            is.na(ECH$LVMI_FLAG), NA, 0)
ECH$StageB_FLAG[ECH$EF_FLAG==1 |
                  ECH$EEprime_FLAG==1 |
                  ECH$LAVI_FLAG==1 |
                  ECH$LVMI_FLAG==1] <- 1

ECH_W <- unique(select(ECH, patient_id))

for (varname in c('EF_FLAG','EEprime_FLAG','LAVI_FLAG','LVMI_FLAG','StageB_FLAG')) {
  #varname='LVMI_FLAG'
  TEMP1 = unique(subset(ECH, !is.na(get(varname)) & !is.na(Ech_date),
                        c(patient_id, get(varname), Ech_date)))
  TEMP1[,varname] = as.numeric(TEMP1[,varname])
  TEMP2 = collapse(TEMP1, c('patient_id','Ech_date'), 'max')
  TEMP3 = renamevar(TEMP2, c('Ech_date'), c(paste(varname,'Date',sep='')))
  TEMP4 = wideorder(TEMP3, c(varname,paste(varname,'Date',sep='')), paste(varname,'Date',sep=''), 'patient_id')
  ECH_W = merge(ECH_W, TEMP4, by='patient_id', all.x=TRUE)
}

ech_list_1 <- c('EF', 'LVIDd', 'LVIDs', 'LV_mass', 'LVMI', 'IVSd',
                'PWd', 'LA_vol', 'LAVI', 'left_atrium',
                'peak_E', 'peak_A', 'EEprime_ratio',
                'LatEprime', 'medialEa', 'EA_ratio')
ech_list_2 <- c('rhythm', 'mvregurg', 'mvsten', 'avregurg', 'avsten', 'tvregurg', 'tvsten')

for (varname in ech_list_1) {
  #varname='rhythm'
  TEMP1 = unique(subset(ECH, !is.na(get(varname)) & !is.na(Ech_date),
                        c(patient_id, get(varname), Ech_date)))
  # TEMP1[,varname] = as.numeric(TEMP1[,varname])
  TEMP2 = collapse(TEMP1, c('patient_id','Ech_date'), 'median')
  TEMP3 = renamevar(TEMP2, c('Ech_date'), c(paste(varname,'Date',sep='')))
  TEMP4 = wideorder(TEMP3, c(varname,paste(varname,'Date',sep='')), paste(varname,'Date',sep=''), 'patient_id')
  ECH_W = merge(ECH_W, TEMP4, by='patient_id', all.x=TRUE)
}

for (varname in ech_list_2) {
  #varname='rhythm'
  TEMP1 = unique(subset(ECH, !is.na(get(varname)) & !is.na(Ech_date),
                        c(patient_id, get(varname), Ech_date)))
  #TEMP1[,varname] = as.numeric(TEMP1[,varname])
  TEMP2 = collapse(TEMP1, c('patient_id','Ech_date'), 'median')
  TEMP3 = renamevar(TEMP2, c('Ech_date'), c(paste(varname,'Date',sep='')))
  TEMP4 = wideorder(TEMP3, c(varname,paste(varname,'Date',sep='')), paste(varname,'Date',sep=''), 'patient_id')
  ECH_W = merge(ECH_W, TEMP4, by='patient_id', all.x=TRUE)
}

# lab tests
TEST_W <- unique(select(filter(LAB_TESTS, patient_id %in% S_DATA$id),patient_id))
LAB_TESTS$testdate <- as.Date(LAB_TESTS$testdate, '1970-01-01')

LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('BNP')] = 'BNP'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('NT-proBNP')] = 'NTP'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('TotalCholesterol')] = 'CHL'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('LDL')] = 'LDL'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('HDL')] = 'HDL'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('Triglycerides')] = 'TGS'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('Glucose')] = 'GLC'
LAB_TESTS$testtype[LAB_TESTS$testtype %in% c('Creatinine')] = 'CRE'

test_list <- c("BNP","NTP","CHL","LDL","HDL","TGS","GLC","CRE","HB","Ferritin")

for (testname in test_list) {
  #testname='BNP'
  TEMP1 = subset(LAB_TESTS, testtype==testname & !is.na(result) & !is.na(testdate),
                 c(patient_id, testdate, result, testtype))
  TEMP1$result = gsub('<|>','',TEMP1$result)
  TEMP1$result = gsub('\\.\\.','\\.',TEMP1$result)
  # TEMP1$result = as.numeric(TEMP1$result)
  #  TEMP1 = subset(TEMP1, !is.na(result))
  TEMP2 = collapse(TEMP1, c('patient_id','testdate'), 'median')
  TEMP3 = renamevar(TEMP2, c('testdate','result'), c(paste(testname,'Date',sep=''),testname))
  TEMP4 = wideorder(TEMP3, c(testname,paste(testname,'Date',sep='')), paste(testname,'Date',sep=''), 'patient_id')
  TEST_W = merge(TEST_W, TEMP4, by='patient_id', all.x=TRUE)
}

# add med class date to W
# aggregate overall data
# ECH_W <- select(ECH_W, -male)
T_DATA <- merge(merge(merge(merge(S_DATA, CLN_W, by.x="id", by.y="patient_id", all.x=TRUE),
                            ECH_W, by.x="id", by.y="patient_id", all.x=TRUE),
                      TEST_W, by.x="id", by.y="patient_id", all.x=TRUE),
                SCRIPTS_W, by.x="id", by.y="patient_id", all.x=TRUE)
# T_DATA <- filter(T_DATA, min_echo_date != max_echo_date)

# datecaliper = round( 6 * (365.25/12),0) # 6 months
datecaliper = 365*10 # 6 months
# BASELINE VALUES

for (v in c("smoking_status","Height","Weight","BMI","HR","SBP","DBP",
            "BNP","NTP","CHL","LDL","HDL","TGS","GLC","CRE","HB","Ferritin",
            'EF', 'LVIDd', 'LVIDs', 'LV_mass', 'LVMI', 'IVSd',
            'PWd', 'LA_vol', 'LAVI', 'left_atrium',
            'peak_E', 'peak_A', 'EEprime_ratio',
            'LatEprime', 'medialEa', 'EA_ratio',
            'rhythm', 'mvregurg', 'mvsten', 'avregurg', 'avsten', 'tvregurg', 'tvsten',
            'EF_FLAG','EEprime_FLAG','LAVI_FLAG','LVMI_FLAG','StageB_FLAG'
)) {
  Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
  for (i in 1:nrow(T_DATA)) {
    bl_date = T_DATA[i,'min_clinic_date']
    tmp_dates = T_DATA[i,paste(v,'Date_n',1:Hi_n,sep='')]
    tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
    # tmp_values = as.numeric(T_DATA[i,paste(v,'_n',1:Hi_n,sep='')])
    tmp_dates = as.Date(tmp_dates[tmp_values!=""])
    tmp_values = tmp_values[tmp_values!=""]
    tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(bl_date))
    if (all(is.na(tmp_values))==TRUE) {
      T_DATA[i,paste(v,'_BL',sep='')] = NA }
    if (all(is.na(tmp_absgaps))==TRUE) {
      T_DATA[i,paste(v,'Date_BL',sep='')] = NA }
    if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
      tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
      tmp_n = match(tmp_minabsgap,tmp_absgaps)
      tmp_thisdate = tmp_thisvalue = NA
      if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
      T_DATA[i,paste(v,'_BL',sep='')] = tmp_thisvalue
      T_DATA[i,paste(v,'Date_BL',sep='')] = as.Date(as.numeric(tmp_thisdate),"1970-01-01")
    }
  }
}

for (v in c("MED_script")) {
  #v="MED_script"
  Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
  for (i in 1:nrow(T_DATA)) {
    #i=2
    bl_date = T_DATA[i,'min_clinic_date']
    tmp_dates = T_DATA[i,paste(v,'Date_n',1:Hi_n,sep='')]
    tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
    tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(bl_date))
    if (all(is.na(tmp_values))==TRUE) {
      T_DATA[i,paste(v,'_BL',sep='')] = NA }
    if (all(is.na(tmp_absgaps))==TRUE) {
      T_DATA[i,paste(v,'Date_BL',sep='')] = NA }
    if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
      tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
      tmp_n = match(tmp_minabsgap,tmp_absgaps)
      tmp_thisdate = tmp_thisvalue = NA
      if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
      T_DATA[i,paste(v,'_BL',sep='')] = tmp_thisvalue
      T_DATA[i,paste(v,'Date_BL',sep='')] = as.Date(as.numeric(tmp_thisdate),'1970-01-01')
    }
  }
}


for (v in c("MED_classes")) {
  #v="MED_classes"
  Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
  for (i in 1:nrow(T_DATA)) {
    #i=1
    bl_date = T_DATA[i,'min_clinic_date']
    tmp_dates = T_DATA[i,paste('MED_script','Date_n',1:Hi_n,sep='')]
    tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
    tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(bl_date))
    if (all(is.na(tmp_values))==TRUE) {
      T_DATA[i,paste(v,'_BL',sep='')] = NA }
    if (all(is.na(tmp_absgaps))==TRUE) {
      T_DATA[i,paste(v,'Date_BL',sep='')] = NA }
    if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
      tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
      tmp_n = match(tmp_minabsgap,tmp_absgaps)
      tmp_thisdate = tmp_thisvalue = NA
      if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
      T_DATA[i,paste(v,'_BL',sep='')] = tmp_thisvalue
      T_DATA[i,paste(v,'Date_BL',sep='')] = as.Date(as.numeric(tmp_thisdate),'1970-01-01')
    }
  }
}

# # FOLLOW UP VALUES
# for (v in c("smoking_status","Height","Weight","BMI","HR","SBP","DBP",
#             "BNP","CHL","LDL","HDL","TGS","GLC","CRE","HB","HbA1C",
#             'EF', 'LVIDd', 'LVIDs', 'LV_mass', 'LVMI', 'IVSd',
#             'PWd', 'LA_vol', 'LAVI', 'left_atrium',
#             'peak_E', 'peak_A', 'EEprime_ratio',
#             'LatEprime', 'medialEa', 'EA_ratio',
#             'rhythm', 'mvregurg', 'mvsten', 'avregurg', 'avsten', 'tvregurg', 'tvsten',
#             'EF_FLAG','EEprime_FLAG','LAVI_FLAG','LVMI_FLAG','StageB_FLAG')) {
#   Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
#   for (i in 1:nrow(T_DATA)) {
#     fu_date = T_DATA[i,'max_echo_date']
#     tmp_dates = T_DATA[i,paste(v,'Date_n',1:Hi_n,sep='')]
#     tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
#     #tmp_values = as.numeric(T_DATA[i,paste(v,'_n',1:Hi_n,sep='')])
#     tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(fu_date))
#     if (all(is.na(tmp_values))==TRUE) {
#       T_DATA[i,paste(v,'_FU',sep='')] = NA }
#     if (all(is.na(tmp_absgaps))==TRUE) {
#       T_DATA[i,paste(v,'Date_FU',sep='')] = NA }
#     if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
#       tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
#       tmp_n = match(tmp_minabsgap,tmp_absgaps)
#       tmp_thisdate = tmp_thisvalue = NA
#       if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
#       T_DATA[i,paste(v,'_FU',sep='')] = tmp_thisvalue
#       T_DATA[i,paste(v,'Date_FU',sep='')] = as.Date(as.numeric(tmp_thisdate),"1970-01-01")
#     }
#   }
# }
# 
# for (v in c("MED_script")) {
#   #v="MED_script"
#   Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
#   for (i in 1:nrow(T_DATA)) {
#     #i=1
#     fu_date = T_DATA[i,'max_echo_date']
#     tmp_dates = T_DATA[i,paste(v,'Date_n',1:Hi_n,sep='')]
#     tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
#     tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(fu_date))
#     if (all(is.na(tmp_values))==TRUE) {
#       T_DATA[i,paste(v,'_FU',sep='')] = NA }
#     if (all(is.na(tmp_absgaps))==TRUE) {
#       T_DATA[i,paste(v,'Date_FU',sep='')] = NA }
#     if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
#       tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
#       tmp_n = match(tmp_minabsgap,tmp_absgaps)
#       tmp_thisdate = tmp_thisvalue = NA
#       if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
#       T_DATA[i,paste(v,'_FU',sep='')] = tmp_thisvalue
#       T_DATA[i,paste(v,'Date_FU',sep='')] = as.Date(as.numeric(tmp_thisdate),'1970-01-01')
#     }
#   }
# }
# 
# for (v in c("MED_classes")) {
#   #v="MED_classes"
#   Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(T_DATA),value=TRUE))))
#   for (i in 1:nrow(T_DATA)) {
#     #i=1
#     fu_date = T_DATA[i,'max_echo_date']
#     tmp_dates = T_DATA[i,paste('MED_script','Date_n',1:Hi_n,sep='')]
#     tmp_values = T_DATA[i,paste(v,'_n',1:Hi_n,sep='')]
#     tmp_absgaps = abs(as.numeric(tmp_dates)-as.numeric(fu_date))
#     if (all(is.na(tmp_values))==TRUE) {
#       T_DATA[i,paste(v,'_FU',sep='')] = NA }
#     if (all(is.na(tmp_absgaps))==TRUE) {
#       T_DATA[i,paste(v,'Date_FU',sep='')] = NA }
#     if (all(is.na(tmp_values))==FALSE & all(is.na(tmp_absgaps))==FALSE) {
#       tmp_minabsgap = min(tmp_absgaps[!is.na(tmp_values)],na.rm=TRUE)
#       tmp_n = match(tmp_minabsgap,tmp_absgaps)
#       tmp_thisdate = tmp_thisvalue = NA
#       if(tmp_minabsgap <= datecaliper) { tmp_thisdate = tmp_dates[tmp_n] ; tmp_thisvalue = tmp_values[tmp_n] }
#       T_DATA[i,paste(v,'_FU',sep='')] = tmp_thisvalue
#       T_DATA[i,paste(v,'Date_FU',sep='')] = as.Date(as.numeric(tmp_thisdate),'1970-01-01')
#     }
#   }
# }

# med_classes <- c("AA", "AB", "ACEI", "ANALGESIA", "ANTICOAG",
#                  "ANTIDIABETIC", "APOTHER", "ARB", "ARRHY", 
#                  "ASPIRIN", "B2A", "BB", "BDZ", "BONE", "CCB", "CG", 
#                  "CHOL", "CLOP", "CNSOTHER", "CVCOTHER", "CVOTHER", 
#                  "DEPR", "DIUROTH", "DMARD", "EYE", "GIOTHER", "GOUT", 
#                  "GU", "H2A", "INF", "ING", "INSULIN", "IS", "IVABRADINE", 
#                  "LOOP", "LYR", "MALIG", "NITRATE", "NSAID", "NUT", "NUTE", 
#                  "NUTR", "OC", "ORALSTER", "OTHER", "PARA", "PPI", "PULM", 
#                  "STERINH", "TCA", "THYR", "TOP", "WARFARIN")

T_DATA$AB_BL <- ifelse(grepl("AB", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$ACEI_BL <- ifelse(grepl("ACEI", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$ARB_BL <- ifelse(grepl("ARB", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$BB_BL <- ifelse(grepl("BB", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$CCB_BL <- ifelse(grepl("CCB", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$DIUROTH_BL <- ifelse(grepl("DIUROTH", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$LOOP_BL <- ifelse(grepl("LOOP", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$ANTICOAG_BL <- ifelse(grepl("ANTICOAG", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$WARFARIN_BL <- ifelse(grepl("WARFARIN", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$ASPIRIN_BL <- ifelse(grepl("ASPIRIN", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$APOTHER_BL <- ifelse(grepl("APOTHER", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$ANTIDIABETIC_BL <- ifelse(grepl("ANTIDIABETIC", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$INSULIN_BL <- ifelse(grepl("INSULIN", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$CHOL_BL <- ifelse(grepl("CHOL", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$IVABRADINE_BL <- ifelse(grepl("IVABRADINE", T_DATA$MED_classes_BL)==TRUE, 1, 0)
T_DATA$DIGOXIN_BL <- ifelse(grepl("DIGOXIN", T_DATA$MED_script_BL, ignore.case = TRUE)==TRUE, 1, 0)

# T_DATA$AB_FU <- ifelse(grepl("AB", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$ACEI_FU <- ifelse(grepl("ACEI", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$ARB_FU <- ifelse(grepl("ARB", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$BB_FU <- ifelse(grepl("BB", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$CCB_FU <- ifelse(grepl("CCB", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$DIUROTH_FU <- ifelse(grepl("DIUROTH", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$LOOP_FU <- ifelse(grepl("LOOP", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$ANTICOAG_FU <- ifelse(grepl("ANTICOAG", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$WARFARIN_FU <- ifelse(grepl("WARFARIN", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$ASPIRIN_FU <- ifelse(grepl("ASPIRIN", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$APOTHER_FU <- ifelse(grepl("APOTHER", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$ANTIDIABETIC_FU <- ifelse(grepl("ANTIDIABETIC", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$INSULIN_FU <- ifelse(grepl("INSULIN", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$CHOL_FU <- ifelse(grepl("CHOL", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$IVABRADINE_FU <- ifelse(grepl("IVABRADINE", T_DATA$MED_classes_FU)==TRUE, 1, 0)
# T_DATA$DIGOXIN_FU <- ifelse(grepl("DIGOXIN", T_DATA$MED_script_FU, ignore.case = TRUE)==TRUE, 1, 0)
# 
# T_DATA$EF_BL <- as.numeric(T_DATA$EF_BL)
# T_DATA$EF_FU <- as.numeric(T_DATA$EF_FU)
# 
# T_DATA$EEprime_ratio_BL <- as.numeric(T_DATA$EEprime_ratio_BL)
# T_DATA$EEprime_ratio_FU <- as.numeric(T_DATA$EEprime_ratio_FU)
# 
# T_DATA$LAVI_BL <- as.numeric(T_DATA$LAVI_BL)
# T_DATA$LAVI_FU <- as.numeric(T_DATA$LAVI_FU)
# 
# T_DATA$LVMI_BL <- as.numeric(T_DATA$LVMI_BL)
# T_DATA$LVMI_FU <- as.numeric(T_DATA$LVMI_FU)
# 
# 
# T_DATA$LVSD_PG <- 0
# T_DATA$LVSD_PG <- ifelse(T_DATA$EF_FU<50 & (T_DATA$EF_FU-T_DATA$EF_BL)>=5, 1, 0)
# 
# T_DATA$LVDD_PG <- 0
# T_DATA$LVDD_PG <- ifelse(T_DATA$EEprime_ratio_FU>13 & (T_DATA$EEprime_ratio_FU-T_DATA$EEprime_ratio_BL)>=5, 1, 0)
# 
# T_DATA$LAVI_PG <- 0
# T_DATA$LAVI_PG <- ifelse(T_DATA$LAVI_FU>34 & (T_DATA$LAVI_FU-T_DATA$LAVI_BL)>=3.4, 1, 0)
# 
# T_DATA$LVMI_PG <- 0
# T_DATA$LVMI_PG <- ifelse(T_DATA$LVMI_FLAG_FU==1 & (T_DATA$LVMI_FU-T_DATA$LVMI_BL)>10, 1, 0)

U_DATA <- select(T_DATA,
                 id, MRN, male, birth_date, min_clinic_date, max_clinic_date, Age_BL, RIP_flag, death_date, patient_type,
                 grep('HeartFailure', names(T_DATA), value=TRUE),
                 grep('DGN',names(T_DATA), value = TRUE),
                 -grep('ANA', names(T_DATA), value = TRUE),
                 -grep('CNC', names(T_DATA), value = TRUE),
                 -grep('PN', names(T_DATA), value = TRUE),
                 -grep('PVD', names(T_DATA), value = TRUE),
                 -grep('THM', names(T_DATA), value = TRUE),
                 -grep('VAL', names(T_DATA), value = TRUE),
                 grep('ADM_EM', names(T_DATA), value=TRUE),
                 grep('ADM_EL', names(T_DATA), value=TRUE),
                 grep('BL', names(T_DATA), value=TRUE))
# grep('FU', names(T_DATA), value=TRUE),
# grep("PG", names(T_DATA), value=TRUE),
# grep('BNP', names(T_DATA), value=TRUE),
# grep('NTproBNP', names(T_DATA), value=TRUE),
# grep('CRE', names(T_DATA), value=TRUE),
# grep('HB', names(T_DATA), value=TRUE),
# grep('Ferritin', names(T_DATA), value=TRUE),
# grep('HDL', names(T_DATA), value=TRUE),
# grep('LDL', names(T_DATA), value=TRUE),
# grep('GLC', names(T_DATA), value=TRUE),
# grep('BP', names(T_DATA), value=TRUE),
# grep('Weight', names(T_DATA), value=TRUE)

U_DATA$birth_date <- as.Date(U_DATA$birth_date, "1970-01-01")
U_DATA$death_date <- as.Date(U_DATA$death_date, "1970-01-01")
# U_DATA$medialEaDate_BL <- as.Date(U_DATA$medialEaDate_BL, "1970-01-01")
# U_DATA$medialEaDate_FU <- as.Date(U_DATA$medialEaDate_FU, "1970-01-01")
# U_DATA$EA_ratioDate_BL <- as.Date(U_DATA$EA_ratioDate_BL, "1970-01-01")
# U_DATA$EA_ratioDate_FU <- as.Date(U_DATA$EA_ratioDate_FU, "1970-01-01")
# 
# U_DATA$HbA1CDate_BL <- as.Date(U_DATA$HbA1CDate_BL,"1970-01-01")
# U_DATA$HbA1CDate_FU <- as.Date(U_DATA$HbA1CDate_FU,"1970-01-01")

# U_DATA <- filter(U_DATA, !is.na(min_echo_date) &
#                          !is.na(max_echo_date) &
#                          min_echo_date != max_echo_date &
#                          !is.na(EF_BL) &
#                          !is.na(EF_FU) &
#                          !is.na(BNPDate_BL) &
#                          !is.na(BNPDate_FU) &
#                          BNPDate_BL != BNPDate_FU &
#                          !is.na(BNP_BL) &
#                          !is.na(BNP_FU))

for (v in c("ADM_EMHFDate","ADM_EMCVDate","ADM_EMXXDate","ADM_ELCVDate","ADM_ELXXDate")) {
  Hi_n = max(as.numeric(gsub(paste(v,'_n',sep=''),'',grep(paste(v,'_n',sep=''),names(U_DATA),value=TRUE))))
  for (j in paste(v,'_n',1:Hi_n,sep='')) {
    U_DATA[,j] <- as.Date(U_DATA[,j],"1970-01-01")
  }
}

write.csv(U_DATA,paste('HFVC_NDC_Programme_Referral_StageB ',Sys.Date(),'.csv',sep=''),row.names=FALSE)

# table(as.Date(as.numeric(M_DATA$ADM_ADXXDate_n1),'1970-01-01'))