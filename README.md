# Youth-Risk-Map
A digital map showing the probability of child endangerment based on the 2011 census in a 100m x 100m radius.

The scripts for data transformation are also included in the repository. (Named after the respective data set.)

**The SQL query (BigQuery) for calculating the risk:**
```sql
DECLARE parent_divorce_weight FLOAT64 DEFAULT 2;
DECLARE unmarried_families_weight FLOAT64 DEFAULT 1.5;
DECLARE overcrowding_household_weight FLOAT64 DEFAULT 1.25;
DECLARE poverty_weight FLOAT64 DEFAULT 1.75;
DECLARE experience_of_death_weight FLOAT64 DEFAULT 1;


CREATE OR REPLACE TABLE `project-name.table-name.main` AS
WITH poi_calculation AS (
    SELECT
    #Like Bavaria or Berlin
    e.federal_state,
    #City or village
    e.district_type,
    #The name of the city or village
    e.district_name,
    #Latitude of the middle of the 100m x 100m area
    e.lat,
    #Longitude of the middle of the 100m x 100m area
    e.lon,
    #All people under 20 years of age are counted as group size
    (IFNULL(d.alter_10jg_unter_10_volume, 0) + IFNULL(d.alter_10jg_10_19_volume, 0)) AS group_size,
    #Calucating all parental divorce by the single parenting rate (most of single parents was married) of both sexes and the summarized divorce rate, which hAS also all non parents in it but also all deviorced parents which nevertheless live together. Because of this, I use the middle of all single parents and all divorced people.
    SAFE_DIVIDE((IFNULL(f.famtyp_kind_alleinerziehende_muetter_mind_1_kind_kleiner_18_volume, 0) + IFNULL(f.famtyp_kind_alleinerziehende_vaeter_mind_1_kind_kleiner_18_volume, 0)) + IFNULL(d.famstnd_ausf_geschieden_volume, 0), 2) AS parent_divorce_count,
    #Get all unmarried families
    IFNULL(f.famtyp_kind_nichteheliche_lebensgem_mind_1_kind_kleiner_18_volume, 0) AS unmarried_families_count,
    #Calculating overcrowded households count by the middle of the big family size and the volume of small/middle apartments. The count is on max if both values are high and min if both values are low. So, a higher result mean a higher probability of overcrowded households
    SAFE_DIVIDE((IFNULL(f.famgroess_klass_5_personen_volume, 0) + IFNULL(f.famgroess_klass_6_und_mehr_personen_volume, 0)) + (IFNULL(h.wohnflaeche_10s_40_49_volume, 0) + IFNULL(h.wohnflaeche_10s_50_59_volume, 0) + IFNULL(h.wohnflaeche_10s_60_69_volume, 0)), 2) AS overcrowding_household_count,
    #Calculating the probable poverty in the area by the count of old houses
    (IFNULL(b.baujahr_mz_vor_1919_volume, 0) + IFNULL(b.baujahr_mz_1919_1948_volume, 0) + IFNULL(b.baujahr_mz_1949_1978_volume, 0)) AS poverty_count,
    #Calculating the expierence of death of loved ones, especially the long-term expierence trough the nearby living of the partner. 1. Calculating the percental share of household with youngs and olds of all households with olds. 2. Multipling the share by the widowed count.
    (SAFE_DIVIDE(IFNULL(f.hhtyp_senior_hh_haushalte_mit_senioren_und_juengeren_volume, 0), (IFNULL(f.hhtyp_senior_hh_haushalte_mit_senioren_und_juengeren_volume, 0) + IFNULL(f.hhtyp_senior_hh_haushalte_mit_ausschliesslich_senioren_volume, 0))) * IFNULL(d.famstnd_ausf_verwitwet_volume, 0)) AS experience_of_death_count,
    #Counts for normalization
    #Married parents count
    (IFNULL(f.famtyp_kind_ehepaare_alle_kinder_ab_18_volume, 0) + IFNULL(f.famtyp_kind_ehepaare_mind_1_kind_kleiner_18_volume, 0)) AS married_parents_count,
    #Married families count
    IFNULL(f.famtyp_kind_alleinerziehende_muetter_mind_1_kind_kleiner_18_volume, 0) + IFNULL(f.famtyp_kind_alleinerziehende_vaeter_mind_1_kind_kleiner_18_volume, 0) + IFNULL(f.famtyp_kind_ehepaare_mind_1_kind_kleiner_18_volume, 0) AS married_families_count,
    #Apartment count
    (IFNULL(h.wohnflaeche_10s_unter_30_volume, 0) + IFNULL(h.wohnflaeche_10s_30_39_volume, 0) + IFNULL(h.wohnflaeche_10s_40_49_volume, 0) + IFNULL(h.wohnflaeche_10s_50_59_volume, 0) + IFNULL(h.wohnflaeche_10s_60_69_volume, 0) + IFNULL(h.wohnflaeche_10s_70_79_volume, 0) + IFNULL(h.wohnflaeche_10s_80_89_volume, 0) + IFNULL(h.wohnflaeche_10s_90_99_volume, 0) + IFNULL(h.wohnflaeche_10s_100_109_volume, 0) + IFNULL(h.wohnflaeche_10s_110_119_volume, 0) + IFNULL(h.wohnflaeche_10s_120_129_volume, 0) + IFNULL(h.wohnflaeche_10s_130_139_volume, 0) + IFNULL(h.wohnflaeche_10s_140_149_volume, 0) + IFNULL(h.wohnflaeche_10s_150_159_volume, 0) + IFNULL(h.wohnflaeche_10s_160_169_volume, 0) + IFNULL(h.wohnflaeche_10s_170_179_volume, 0) + IFNULL(h.wohnflaeche_10s_180_und_mehr_volume, 0)) AS apartment_count,
    #House count
    (IFNULL(b.baujahr_mz_vor_1919_volume, 0) + IFNULL(b.baujahr_mz_1919_1948_volume, 0) + IFNULL(b.baujahr_mz_1949_1978_volume, 0) + IFNULL(b.baujahr_mz_1979_1986_volume, 0) + IFNULL(b.baujahr_mz_1987_1990_volume, 0) + IFNULL(b.baujahr_mz_1991_1995_volume, 0) + IFNULL(b.baujahr_mz_1996_2000_volume, 0) + IFNULL(b.baujahr_mz_2001_2004_volume, 0) + IFNULL(b.baujahr_mz_2005_2008_volume, 0) + IFNULL(b.baujahr_mz_2009_und_spaeter_volume, 0)) AS house_count,
    #Population count
    IFNULL(p.population_volume, 0) AS population_count
  FROM
    `project-name.table-name.demographic` AS d
  INNER JOIN
    `project-name.table-name.etrs89_encoding` AS e
  ON
    d.id = e.id
  INNER JOIN
    `project-name.table-name.families` AS f
  ON
    f.id = d.id
  INNER JOIN
    `project-name.table-name.homes` AS h
  ON
    h.id = d.id
  INNER JOIN
    `project-name.table-name.buildings` AS b
  ON
    b.id = d.id
  INNER JOIN
    `project-name.table-name.population` AS p
  ON
    p.id = d.id
  #Selecting targeted group
  WHERE
    (d.alter_10jg_unter_10_volume IS NOT NULL AND d.alter_10jg_unter_10_quality < 2
    AND d.alter_10jg_unter_10_volume > 0) OR (d.alter_10jg_10_19_volume IS NOT NULL AND d.alter_10jg_10_19_quality < 2
    AND d.alter_10jg_10_19_volume > 0)
),

poi_normalization AS (
  SELECT
  federal_state,
  district_type,
  district_name,
  lon,
  lat,
  group_size,
  IFNULL(SAFE_DIVIDE(parent_divorce_count, (parent_divorce_count + married_parents_count)), 0) AS parent_divorce_rate,
  IFNULL(SAFE_DIVIDE(unmarried_families_count, (unmarried_families_count + married_families_count)), 0) AS unmarried_families_rate,
  IFNULL(SAFE_DIVIDE(overcrowding_household_count, apartment_count), 0) AS overcrowding_household_rate,
  IFNULL(SAFE_DIVIDE(poverty_count, house_count), 0) AS poverty_rate,
  IFNULL(SAFE_DIVIDE(experience_of_death_count, population_count), 0) AS experience_of_death_rate
  FROM poi_calculation
),

poi AS (
  SELECT
  federal_state,
  district_type,
  district_name,
  lon,
  lat,
  group_size,
  parent_divorce_rate,
  unmarried_families_rate,
  overcrowding_household_rate,
  poverty_rate,
  experience_of_death_rate,
  SAFE_DIVIDE((parent_divorce_rate * parent_divorce_weight + unmarried_families_rate * unmarried_families_weight + overcrowding_household_rate * overcrowding_household_weight + poverty_rate * poverty_weight + experience_of_death_rate * experience_of_death_weight), (5 + ((parent_divorce_weight + unmarried_families_weight + overcrowding_household_weight + poverty_weight + experience_of_death_weight) - 5 ))) AS risk_rate
  FROM poi_normalization
)

SELECT
  federal_state,
  district_type,
  district_name,
  lon,
  lat,
  group_size,
  parent_divorce_rate,
  unmarried_families_rate,
  overcrowding_household_rate,
  poverty_rate,
  experience_of_death_rate,
  risk_rate,
  CASE
    WHEN risk_rate >= 0 AND risk_rate < .15 THEN "1. GREEN"
    WHEN risk_rate >= .15 AND risk_rate < .25 THEN "2. YELLOW"
    WHEN risk_rate >= .25 AND risk_rate < .35 THEN "3. ORANGE"
    WHEN risk_rate >= .35 AND risk_rate < .45 THEN "4. RED"
    WHEN risk_rate >= .45 AND risk_rate < .55 THEN "5. BLUE"
    WHEN risk_rate >= .55 THEN "6. WHITE"
  END AS segment_name
FROM poi;
```
