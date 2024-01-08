library(tidycensus)
library(tidyverse)
library(janitor)
library(readxl)
library(jsonlite)
library(tidylog)
library(leaflet)
library(sf)
library(kableExtra)
library(stringr)
library(reshape)
library(lubridate)
library(fpp2)
library(zoo)
library(googlesheets4)
library(googledrive)
library(gargle)
library(httr)

# google authentication

drive_auth(path = "file goes here")
gs4_auth(path = "file goes here")

# census population data

vs <- load_variables(2018, "acs5")

county_pop <- get_acs(geography = "county",
                      variables = c(pop = "B01003_001"),
                      state = "CA",
                      geometry = FALSE)

county_pop_edited <- county_pop %>%
  dplyr::select(NAME, estimate) %>%
  tidyr::separate(NAME, into = c("county", "state"), sep = ", ") %>%
  dplyr::rename(population = estimate) %>%
  dplyr::select(county, population) %>%
  filter(county %in% c("Alameda County", "Contra Costa County", "Marin County", "Napa County", "San Francisco County", "San Mateo County", "Santa Clara County", "Solano County", "Sonoma County", "Santa Cruz County", "Mendocino County", "Lake County", "Monterey County", "San Benito County"))

# case rates

#covid_daily_info <- read_sheet("https:/docs.google.com/spreadsheets/d/1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA/edit#gid=1487251137")

covid_daily_info <- read_csv("https://data.chhs.ca.gov/dataset/f333528b-4d38-4814-bebb-12db1f10f535/resource/046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a/download/covid19cases_test.csv")

ba_counties <- c("Alameda", "Contra Costa", "Marin", "San Francisco", "San Mateo", "Santa Clara", "Napa", "Santa Cruz", "Solano", "Sonoma")

covid_daily_info1 <- covid_daily_info %>%
  dplyr::select(date, area, cases, cumulative_cases, deaths, cumulative_deaths) %>%
  filter(area %in% ba_counties) %>%
  filter(date >= "2020-03-01") %>%
  dplyr::rename("County" = "area") %>%
  dplyr::rename("Date" = "date") %>%
  dplyr::rename("Daily Cases" = "cases") %>%
  dplyr::rename("cases" = "cumulative_cases") %>%
  dplyr::rename("Daily Deaths" = "deaths") %>%
  dplyr::rename("deaths" = "cumulative_deaths") %>%
  pivot_wider(names_from = County, values_from = c(cases, deaths, `Daily Cases`, `Daily Deaths`), names_glue = "{County} {.value}") %>%
  mutate(`Cumulative Cases` = (`Alameda cases` + `Contra Costa cases` + `Marin cases` + `San Francisco cases` + `San Mateo cases` + `Santa Clara cases` + `Napa cases` + `Santa Cruz cases` + `Solano cases` + `Sonoma cases`)) %>%
  mutate(`Cumulative Deaths` = (`Alameda deaths` + `Contra Costa deaths` + `Marin deaths` + `San Francisco deaths` + `San Mateo deaths` + `Santa Clara deaths` + `Napa deaths` + `Santa Cruz deaths` + `Solano deaths` + `Sonoma deaths`)) %>%
  mutate(`Total Daily Cases` = (`Alameda Daily Cases` + `Contra Costa Daily Cases` + `Marin Daily Cases` + `San Francisco Daily Cases` + `San Mateo Daily Cases` + `Santa Clara Daily Cases` + `Napa Daily Cases` + `Santa Cruz Daily Cases` + `Solano Daily Cases` + `Sonoma Daily Cases`)) %>%
  mutate(`Total Daily Deaths` = (`Alameda Daily Deaths` + `Contra Costa Daily Deaths` + `Marin Daily Deaths` + `San Francisco Daily Deaths` + `San Mateo Daily Deaths` + `Santa Clara Daily Deaths` + `Napa Daily Deaths` + `Santa Cruz Daily Deaths` + `Solano Daily Deaths` + `Sonoma Daily Deaths`))

Sys.sleep(20)

sheet_write(covid_daily_info1,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Cases and Deaths (state data)")

current_stats <- tail(covid_daily_info1, 1)

keycol1 <- "County"
valuecol1 <- "Case Count"
gathercols1 <- c("Alameda County", "Contra Costa County", "Marin County", "Napa County", "San Francisco County", "San Mateo County", "Santa Clara County", "Santa Cruz County", "Solano County", "Sonoma County")

current_stats_cases <- current_stats %>%
  dplyr::select("Alameda cases",	"Contra Costa cases",	"Marin cases",	"Napa cases",	"San Francisco cases",	"San Mateo cases",	"Santa Clara cases",	"Santa Cruz cases",	"Solano cases",	"Sonoma cases") %>%
  dplyr::rename("Alameda County" = "Alameda cases", "Contra Costa County" =	"Contra Costa cases", "Marin County" =	"Marin cases", "Napa County" =	"Napa cases", "San Francisco County" =	"San Francisco cases", "San Mateo County" =	"San Mateo cases", "Santa Clara County" =	"Santa Clara cases", "Santa Cruz County" =	"Santa Cruz cases", "Solano County" = "Solano cases", "Sonoma County" = "Sonoma cases") %>%
  gather_(keycol1, valuecol1, gathercols1)

county_pop_edited_10 <- county_pop_edited[-c(3,5,6,8), ]

cases_and_pop <- left_join(current_stats_cases, county_pop_edited_10, by = c("County" = "county"))

final_case_stats <- cases_and_pop %>%
  mutate(cases_per_100k = 100000 * (`Case Count` / population)) %>%
  dplyr::select(County, `Case Count`, cases_per_100k) %>%
  dplyr::rename(`Cases Per 100K` = cases_per_100k) %>%
  arrange(desc(`Case Count`))

final_case_stats$`Cases Per 100K` <- format(round(final_case_stats$`Cases Per 100K`, digits = 0))

#final_case_stats$County[final_case_stats$County == "Marin County"] <- "Marin County (not including San Quentin State Prison)"

covid_daily_info_cases <- covid_daily_info1 %>%
  dplyr::select("Date", "Alameda Daily Cases",	"Contra Costa Daily Cases",	"Marin Daily Cases",	"Napa Daily Cases",	"San Francisco Daily Cases",	"San Mateo Daily Cases",	"Santa Clara Daily Cases",	"Santa Cruz Daily Cases",	"Solano Daily Cases",	"Sonoma Daily Cases") %>%
  dplyr::rename("Alameda County" = "Alameda Daily Cases", "Contra Costa County" =	"Contra Costa Daily Cases", "Marin County" =	"Marin Daily Cases", "Napa County" =	"Napa Daily Cases", "San Francisco County" =	"San Francisco Daily Cases", "San Mateo County" =	"San Mateo Daily Cases", "Santa Clara County" =	"Santa Clara Daily Cases", "Santa Cruz County" =	"Santa Cruz Daily Cases", "Solano County" = "Solano Daily Cases", "Sonoma County" = "Sonoma Daily Cases") %>%
  filter(Date >= today() - months(12)) %>%
  mutate(`Alameda County` = rollmean(`Alameda County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Contra Costa County` = rollmean(`Contra Costa County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Marin County` = rollmean(`Marin County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Napa County` = rollmean(`Napa County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`San Francisco County` = rollmean(`San Francisco County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`San Mateo County` = rollmean(`San Mateo County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Santa Clara County` = rollmean(`Santa Clara County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Santa Cruz County` = rollmean(`Santa Cruz County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Solano County` = rollmean(`Solano County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Sonoma County` = rollmean(`Sonoma County`, k = 7, fill = NA, na.rm = TRUE))
  #mutate_all(~na_if(., 0)) %>%
  #fill(`Alameda County`:`Sonoma County`)

covid_daily_info_cases1 <- covid_daily_info_cases %>% gather(County, Cases, `Alameda County`:`Sonoma County`)

covid_daily_info_cases2 <- covid_daily_info_cases1 %>%
  spread(Date, Cases)

#covid_daily_info_cases2$County[covid_daily_info_cases2$County == "Marin County"] <- "Marin County (not including San Quentin State Prison)"

stats_with_daily_cases <- left_join(final_case_stats, covid_daily_info_cases2, by = c("County" = "County"))

Sys.sleep(10)

sheet_write(stats_with_daily_cases,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Case Rates")

# death rates

keycol2 <- "County"
valuecol2 <- "Death Count"
gathercols2 <- c("Alameda County", "Contra Costa County", "Marin County", "Napa County", "San Francisco County", "San Mateo County", "Santa Clara County", "Santa Cruz County", "Solano County", "Sonoma County")

current_stats_deaths <- current_stats %>%
  dplyr::select("Alameda deaths",	"Contra Costa deaths",	"Marin deaths",	"Napa deaths",	"San Francisco deaths",	"San Mateo deaths",	"Santa Clara deaths",	"Santa Cruz deaths",	"Solano deaths",	"Sonoma deaths") %>%
  dplyr::rename("Alameda County" = "Alameda deaths", "Contra Costa County" =	"Contra Costa deaths", "Marin County" =	"Marin deaths", "Napa County" =	"Napa deaths", "San Francisco County" =	"San Francisco deaths", "San Mateo County" =	"San Mateo deaths", "Santa Clara County" =	"Santa Clara deaths", "Santa Cruz County" =	"Santa Cruz deaths", "Solano County" = "Solano deaths", "Sonoma County" = "Sonoma deaths") %>%
  gather_(keycol2, valuecol2, gathercols2)

deaths_and_pop <- left_join(current_stats_deaths, county_pop_edited_10, by = c("County" = "county"))

final_death_stats <- deaths_and_pop %>%
  mutate(deaths_per_100k = 100000 * (`Death Count` / population)) %>%
  dplyr::select(County, `Death Count`,  deaths_per_100k) %>%
  dplyr::rename(`Deaths Per 100K` = deaths_per_100k) %>%
  arrange(desc(`Death Count`))

final_death_stats$`Deaths Per 100K` <- format(round(final_death_stats$`Deaths Per 100K`, digits = 0))

#final_death_stats$County[final_death_stats$County == "Marin County"] <- "Marin County (not including San Quentin State Prison)"

covid_daily_info_deaths <- covid_daily_info1 %>%
  dplyr::select("Date", "Alameda Daily Deaths",	"Contra Costa Daily Deaths",	"Marin Daily Deaths",	"Napa Daily Deaths",	"San Francisco Daily Deaths",	"San Mateo Daily Deaths",	"Santa Clara Daily Deaths",	"Santa Cruz Daily Deaths",	"Solano Daily Deaths",	"Sonoma Daily Deaths") %>%
  dplyr::rename("Alameda County" = "Alameda Daily Deaths", "Contra Costa County" =	"Contra Costa Daily Deaths", "Marin County" =	"Marin Daily Deaths", "Napa County" =	"Napa Daily Deaths", "San Francisco County" =	"San Francisco Daily Deaths", "San Mateo County" =	"San Mateo Daily Deaths", "Santa Clara County" =	"Santa Clara Daily Deaths", "Santa Cruz County" =	"Santa Cruz Daily Deaths", "Solano County" = "Solano Daily Deaths", "Sonoma County" = "Sonoma Daily Deaths") %>%
  filter(Date >= today() - months(12)) %>%
  mutate(`Alameda County` = rollmean(`Alameda County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Contra Costa County` = rollmean(`Contra Costa County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Marin County` = rollmean(`Marin County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Napa County` = rollmean(`Napa County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`San Francisco County` = rollmean(`San Francisco County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`San Mateo County` = rollmean(`San Mateo County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Santa Clara County` = rollmean(`Santa Clara County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Santa Cruz County` = rollmean(`Santa Cruz County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Solano County` = rollmean(`Solano County`, k = 7, fill = NA, na.rm = TRUE)) %>%
  mutate(`Sonoma County` = rollmean(`Sonoma County`, k = 7, fill = NA, na.rm = TRUE))
#mutate_all(~na_if(., 0)) %>%
#fill(`Alameda County`:`Sonoma County`)

covid_daily_info_deaths1 <- covid_daily_info_deaths %>% gather(County, Deaths, `Alameda County`:`Sonoma County`)

covid_daily_info_deaths2 <- covid_daily_info_deaths1 %>%
  spread(Date, Deaths)

#covid_daily_info_deaths2$County[covid_daily_info_deaths2$County == "Marin County"] <- "Marin County (not including San Quentin State Prison)"

stats_with_daily_deaths <- left_join(final_death_stats, covid_daily_info_deaths2, by = c("County" = "County"))

Sys.sleep(10)

sheet_write(stats_with_daily_deaths,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Death Rates")

# daily cases

covid_daily_info2 <- covid_daily_info1 %>%
  dplyr::select(Date, "Total Daily Cases") %>%
  dplyr::rename("Daily Cases" = "Total Daily Cases")

covid_daily_info2[covid_daily_info2 < 0] <- NA

covid_daily_info3 <- covid_daily_info2 %>%
  mutate(`Rolling average` = rollmean(`Daily Cases`, k = 7, fill = NA, na.rm = TRUE))

covid_daily_info3$Date <- as.Date(covid_daily_info3$Date, format = "%Y-%m-%d")

Sys.sleep(10)

sheet_write(covid_daily_info3,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Daily Cases Output")

# daily deaths

covid_daily_info_deaths <- covid_daily_info1 %>%
  dplyr::select(Date, "Total Daily Deaths") %>%
  dplyr::rename("Daily Deaths" = "Total Daily Deaths")

covid_daily_info_deaths[covid_daily_info_deaths < 0] <- NA

covid_daily_info_deaths1 <- covid_daily_info_deaths %>%
  mutate(`Rolling average` = rollmean(`Daily Deaths`, k = 7, fill = NA, na.rm = TRUE))

covid_daily_info_deaths1$Date <- as.Date(covid_daily_info_deaths1$Date, format = "%Y-%m-%d")

Sys.sleep(10)

sheet_write(covid_daily_info_deaths1,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Daily Deaths Output")

# hospitalizations per 100K

hospital_cases <- read_csv("https://data.chhs.ca.gov/dataset/2df3e19e-9ee4-42a6-a087-9761f82033f6/resource/47af979d-8685-4981-bced-96a6b79d3ed5/download/covid19hospitalbycounty.csv")

counties <- c("Alameda", "Contra Costa", "Marin", "San Francisco", "San Mateo", "Santa Clara", "Napa", "Solano", "Sonoma")

hospital_cases1 <- hospital_cases %>%
  dplyr::select("county", "todays_date","hospitalized_covid_confirmed_patients") %>%
  dplyr::rename(hospitalized = hospitalized_covid_confirmed_patients) %>%
  dplyr::rename(Date = todays_date) %>%
  filter(county %in% counties) %>%
  filter(Date > today() - months(12)) %>%
  spread(county, hospitalized)

hospital_cases2 <- hospital_cases1 %>%
  mutate(`Alameda` = (Alameda / 1643700) * 100000) %>%
  mutate(`Contra Costa` = (`Contra Costa` / 1133247)  * 100000) %>%
  mutate(`Marin` = (Marin / 260295)  * 100000) %>%
  mutate(`San Francisco` = (`San Francisco` / 870044)  * 100000) %>%
  mutate(`San Mateo` = (`San Mateo` / 765935)  * 100000) %>%
  mutate(`Santa Clara` = (`Santa Clara` / 1922200)  * 100000) %>%
  mutate(`Napa` = (`Napa` / 140530)  * 100000) %>%
  mutate(`Solano` = (`Solano` / 438530)  * 100000) %>%
  mutate(`Sonoma` = (`Sonoma` / 501317)  * 100000)

Sys.sleep(10)

sheet_write(hospital_cases2,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "County Hospitalizations")

# total hospitalizations vs ICU patients

counties_plus_cruz <- c("Alameda", "Contra Costa", "Marin", "San Francisco", "San Mateo", "Santa Clara", "Napa", "Solano", "Sonoma", "Santa Cruz")

total_hospital_cases <- hospital_cases %>%
  dplyr::select("county", "todays_date","hospitalized_covid_confirmed_patients", "icu_covid_confirmed_patients") %>%
  dplyr::rename(`Total Hospitalizations` = hospitalized_covid_confirmed_patients) %>%
  dplyr::rename(Date = todays_date) %>%
  dplyr::rename(`ICU Patients` = icu_covid_confirmed_patients) %>%
  filter(county %in% counties_plus_cruz) %>%
  filter(Date > "2020-5-20")

BA_hospitalized <- aggregate(total_hospital_cases["Total Hospitalizations"], by=total_hospital_cases["Date"], sum)

BA_ICU <- aggregate(total_hospital_cases["ICU Patients"], by=total_hospital_cases["Date"], sum)

BA_total_hospital_cases <- left_join(BA_hospitalized, BA_ICU, by = c("Date" = "Date"))

Sys.sleep(20)

sheet_write(BA_total_hospital_cases ,ss = "1fn6n36YnAVWHhlWO8xaN83PaGfMFLBOc9db7BfRZSlA", sheet = "Total ICU/Hospitalizations")
