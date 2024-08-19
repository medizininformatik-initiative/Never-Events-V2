### Preparation

setwd("xxx")

start <- Sys.time()

oldw <- getOption("warn")
options(warn = -1)

#load/install a packages
source("install_R_packages.R")


library(data.table)

#create directories

if(!dir.exists("Summary")){dir.create("Summary")}
if(!dir.exists("errors")){dir.create("errors")}
if(!dir.exists("Bundles")){dir.create("Bundles")}


#read  config
if(file.exists("config.yml")){
  conf <- config::get(file = "config.yml")
}else{
  conf <- config::get(file = "config_default.yml")
}

if(!(conf$ssl_verify_peer)){httr::set_config(httr::config(ssl_verifypeer = 0L))}


brackets = c("[", "]")
sep = " || "
############Data extraction#############################
#configure the fhir search url
encounter_request <- fhir_url(url = conf$serverbase,
                              resource = "Encounter",
                              parameters = c("date" = "ge2018-01-01",
                                             "diagnosis:Condition.code"="T81,T81.1,T81.2,T81.4,T81.3,T81.5,T81.6,T81.7,T81.8,T81.9,T88,T88.8,T88.9,Y69,Y84,Y84.9",
                                             "_include" = "Encounter:patient",
                                             "_include"="Encounter:diagnosis"
                              ))

# design parameter for Patient, Encounter, condition and procedure  resources as per fhir_crack function requirement
patients <- fhir_table_description(resource = "Patient",
                                   cols = c(patient_id = "id",
                                            gender        = "gender",
                                            birthdate     = "birthDate"),
                                   style = fhir_style(sep=sep,
                                                      brackets = brackets,
                                                      rm_empty_cols = FALSE)
)



encounters <- fhir_table_description(resource = "Encounter",
                                     cols = c(encounter_id = "id",
                                              admission_date= "period/start",
                                              discharge_date= "period/end",
                                              condition_id ="diagnosis/condition/reference",
                                              patient_id = "subject/reference",
                                              patient_type_fhir_class ="class/code",
                                              diagnosis_use = "diagnosis/use/coding/code",
                                              discharge_reason = "hospitalization/dischargeDisposition/coding/code",
                                              rank = "diagnosis/rank"),
                                     style = fhir_style(sep=sep,
                                                        brackets = brackets,
                                                        rm_empty_cols = FALSE)
)


condition <- fhir_table_description(resource = "Condition",
                                    cols = c(condition_id = "id",
                                             icd = "code/coding/code",
                                             onset_date = "onsetDateTime",
                                             system         = "code/coding/system",
                                             encounter_id = "encounter/reference",
                                             patient_id     = "subject/reference"),
                                    style = fhir_style(sep=sep,
                                                       brackets = brackets,
                                                       rm_empty_cols = FALSE)
)

procedure <- fhir_table_description(resource = "Procedure",
                                    cols = c(procedure_id = "id",
                                             performed_date= "performedDateTime",
                                             ops = "code/coding/code",
                                             system         = "code/coding/system",
                                             encounter_id = "encounter/reference",
                                             patient_id     = "subject/reference"),
                                    style = fhir_style(sep=sep,
                                                       brackets = brackets,
                                                       rm_empty_cols = FALSE)
)

#download the bundles bit by bit and crack them immediately
start_time <- Sys.time()
combined_tables <- list(enc=data.table(), pat = data.table(), con = data.table(), pro = data.table())

while(!is.null(encounter_request)&&length(encounter_request)>0){
  enc_bundles <- fhir_search(request = encounter_request, username = conf$user, password = conf$password, 
                             verbose = 2,log_errors = "errors/encounter_error.xml", max_bundles = 100)
  
  enc_tables <- fhir_crack(enc_bundles, 
                           design = fhir_design(enc = encounters, pat = patients, con = condition, pro = procedure),
                           data.table = TRUE)
  
  combined_tables <- lapply(names(combined_tables), 
                            function(name){
                              rbind(combined_tables[[name]], enc_tables[[name]], fill=TRUE)
                            })
  
  names(combined_tables) <- names(enc_tables)
  
  encounter_request <- fhir_next_bundle_url()
}
#fhir_save(bundles = enc_bundles, directory = "Bundles/Encounters")
rm(enc_bundles, enc_tables)

end_time <- Sys.time()
print(end_time - start_time)

#procedures

procedure_request <- fhir_url(url = conf$serverbase,
                              resource = "Patient",
                              parameters = c("date" = "ge2018-01-01",
                                             "_has:Condition:patient:code"="T81,T81.1,T81.2,T81.4,T81.3,T81.5,T81.6,T81.7,T81.8,T81.9,T88,T88.8,T88.9,Y69,Y84,Y84.9",
#                                             "_include" = "Procedure:encounter",
                                             "_revinclude"="Procedure:subject"
                              ))

start_time <- Sys.time()

while(!is.null(procedure_request)&&length(procedure_request)>0){
  pro_bundles <- fhir_search(request = procedure_request, username = conf$user, password = conf$password, 
                             verbose = 2,log_errors = "errors/procedure_error.xml", max_bundles = 100)
  
  pro_tables <- fhir_crack(pro_bundles, 
                           design = fhir_design(enc = encounters, pat = patients, con = condition, pro = procedure),
                           data.table = TRUE)
  
  combined_tables <- lapply(names(combined_tables), 
                            function(name){
                              rbind(combined_tables[[name]], pro_tables[[name]], fill=TRUE)
                            })
  
  names(combined_tables) <- names(pro_tables)
  
  procedure_request <- fhir_next_bundle_url()
}
#fhir_save(bundles = enc_bundles, directory = "Bundles/Encounters")
rm(pro_bundles, pro_tables)

end_time <- Sys.time()
print(end_time - start_time)

#extract condition resource based on the list of ICD and include Condition:encounter, Condition:subject/patient


### extraction for covering the cases who doesn't have a link of condition in the encounter resource
condition_request_2 <- fhir_url(url = conf$serverbase, 
                                resource = "Condition", 
                                parameters = c("recorded-date" = "ge2018-01-01",
                                               "code"="T81,T81.1,T81.2,T81.4,T81.3,T81.5,T81.6,T81.7,T81.8,T81.9,T88,T88.8,T88.9,Y69,Y84,Y84.9",
                                               "_include" = "Condition:encounter",
                                               "_include"="Condition:subject"
                                ))

#Download in 100er batches and crack immediately, then append to combined tables from above
start_time <- Sys.time()
while(!is.null(condition_request_2)&&length(condition_request_2)>0){
  con_bundles <- fhir_search(request = condition_request_2, username = conf$user, password = conf$password, 
                             verbose = 2,log_errors = "errors/encounter_error.xml", max_bundles = 100)
  
  con_tables <- fhir_crack(con_bundles, 
                           design = fhir_design(enc = encounters, pat = patients, con = condition, pro = procedure),
                           data.table = TRUE)
  
  combined_tables <- lapply(names(combined_tables), 
                            function(name){
                              rbind(combined_tables[[name]], con_tables[[name]], fill=TRUE)
                            })
  
  names(combined_tables) <- names(con_tables)
  
  #get rid of duplicates
  combined_tables$enc <- unique(combined_tables$enc)
  combined_tables$con <- unique(combined_tables$con)
  combined_tables$pat <- unique(combined_tables$pat)
  combined_tables$pro <- unique(combined_tables$pro)
  
  condition_request_2 <- fhir_next_bundle_url()
}

end_time <- Sys.time()
print(end_time - start_time)

#############################

if(nrow(combined_tables$enc) == 0){
  write("Could not find any encounter resource in the server for the required stroke condition. Query Stopped.", file ="errors/error_message.txt")
  stop("No encounters found - aborting.")
}

if(nrow(combined_tables$pat) == 0){
  write("Could not find any patient resource in the server for the required stroke condition. Query Stopped.", file ="errors/error_message.txt")
  stop("No Patients found - aborting.")
}

rm(con_bundles,con_tables)


###############extract and process patient resource##############################
df.patients <- combined_tables$pat
df.patients <- fhir_rm_indices(df.patients, brackets = brackets )
df.patients <- df.patients[!duplicated(df.patients),]


################extract encounter resource###############

df.encounters <- combined_tables$enc
df.encounters <- fhir_melt(df.encounters,
                           columns = c('condition_id' ,'rank','diagnosis_use'),
                           brackets = brackets, sep = sep, all_columns = TRUE)
df.encounters <- fhir_rm_indices(df.encounters, brackets = brackets )


df.encounters$condition_id <- sub("Condition/", "", df.encounters$condition_id)
df.encounters$patient_id <- sub("Patient/", "", df.encounters$patient_id)

df.encounters$admission_date <- as.POSIXct(df.encounters$admission_date,format="%Y-%m-%dT%H:%M:%S")
df.encounters$discharge_date <- as.POSIXct(df.encounters$discharge_date,format="%Y-%m-%dT%H:%M:%S")

df.encounters.trunc <- subset(df.encounters,select = c(patient_id,encounter_id,admission_date,discharge_date))
df.encounters.trunc <- df.encounters.trunc[!duplicated(df.encounters.trunc),]


####################extract the condition resources##################

df.conditions <- combined_tables$con
df.conditions <- fhir_melt(df.conditions, columns = c("icd", "system"), brackets = brackets, sep = sep, all_columns = TRUE)
df.conditions <- fhir_melt(df.conditions, columns = c("icd", "system"), brackets = brackets, sep = sep, all_columns = TRUE)

df.conditions <- fhir_rm_indices(df.conditions, brackets = brackets )
df.conditions$encounter_id <- sub("Encounter/", "", df.conditions$encounter_id)
df.conditions$patient_id <- sub("Patient/", "", df.conditions$patient_id)

df.conditions <- df.conditions[grepl("icd-10", system)]

icd_codes <- c('T81','T81.1','T81.2','T81.4','T81.3','T81.5','T81.6','T81.7','T81.8','T81.9','T88','T88.8','T88.9','Y69','Y84','Y84.9')

df.conditions <- df.conditions[c(which(df.conditions$icd %in% icd_codes) )]

df.conditions$onset_date <- as.POSIXct(df.conditions$onset_date ,format="%Y-%m-%dT%H:%M:%S")

df.encounters.subset <-  df.encounters[,c("encounter_id", "condition_id")]
setnames(df.encounters.subset,old = c("encounter_id", "condition_id"),new = c("encounter.encounter_id", "encounter.condition_id"))
df.conditions <- merge.data.table(x = df.conditions, 
                                  y = df.encounters.subset ,
                                  by.x = "condition_id",
                                  by.y = "encounter.condition_id",
                                  all.x = TRUE)

df.conditions[is.na(encounter_id),encounter_id:=encounter.encounter_id]
df.conditions[, encounter.encounter_id:=NULL]



####################extract the procedure resources##################

df.procedures <- combined_tables$pro
df.procedures <- fhir_melt(df.procedures, columns = c("ops", "system"), brackets = brackets, sep = sep, all_columns = TRUE)
df.procedures <- fhir_melt(df.procedures, columns = c("ops", "system"), brackets = brackets, sep = sep, all_columns = TRUE)

df.procedures <- fhir_rm_indices(df.procedures, brackets = brackets )
df.procedures$encounter_id <- sub("Encounter/", "", df.procedures$encounter_id)
df.procedures$patient_id <- sub("Patient/", "", df.procedures$patient_id)

df.procedures$performed_date <- as.POSIXct(df.procedures$performed_date ,format="%Y-%m-%dT%H:%M:%S")

df.procedures <- df.procedures[, -7]


complete <- left_join(df.encounters, df.procedures, by = "encounter_id")
dup <- which(duplicated(complete) == TRUE)


data <- left_join(df.patients,df.procedures, by = "patient_id")

data <- unique(data)

data <- left_join(data, df.conditions, by = "encounter_id", relationship = "many-to-many")

data <- left_join(data, df.encounters.trunc, by = "encounter_id", relationship = "many-to-many")

data$system.y <- NULL
data$patient_id.y <- NULL
data$resource_identifier <- NULL
data$patient_id <- NULL
data$system.x <- NULL

colnames(data)[1] <- "patient_id"

data <- unique(data)

data$birthdate <- as.numeric(difftime(as.Date("2024-01-01"),as.Date(data$birthdate), units = "days")/365.2422)

names(data)[names(data) == "birthdate"] <- "age"

options(warn = oldw)

fwrite(data, "data.csv")
