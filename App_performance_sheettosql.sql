WITH GAM AS(
  SELECT 
  Case 
  WHen regexp_contains(GAM_db.Ad_unit,'禄') THEN REGEXP_EXTRACT(GAM_db.Ad_unit, '禄 (.*)') 
  ELSE GAM_db.Ad_unit
  END AS AdX_adunit,
GAM_db.Ad_unit as Ad_unit,
GAM_db.Date as Date,
GAM_db.Total_code_served_count as Total_Code_Served_Count,
GAM_db.Unfilled_impressions as Unfilled_impressions,
GAM_db.Total_impressions as Impressions,
GAM_db.Total_clicks as Clicks,
GAM_db.Total_CPM__CPC__CPD__and_vCPM_revenue____ as Revenue,
GAM_db.Total_average_eCPM____ as CPM,
GAM_db.Total_ad_requests as AR,
GAM_db.Total_fill_rate as Fill_rate
FROM `anymanager-playground.GogoLook_db.Data3_GAM`  as GAM_db
),
Mapping AS
(
 SELECT 
  mapping.string_field_0 as ad_unit,
  mapping.string_field_1 as Mediation_group,
  mapping.string_field_2 as Ad_source,
  mapping.string_field_3 as Ad_source_instance,
  mapping.string_field_4 as GAM_Ad_unit,
  mapping.string_field_5 as UPR_name
 FROM `anymanager-playground.GogoLook_db.Mapping` AS mapping
),
Mobx_mapping1 AS(
SELECT 
*,
CASE
  WHEN regexp_contains(Mobx.Ad_source_instance,'␟')
     THEN REGEXP_EXTRACT(Mobx.Ad_source_instance, r'^([^␟]*)␟')
  ELSE Mobx.Ad_source_instance
END AS Layer
FROM `anymanager-playground.GogoLook_db.Data1_Admob` as Mobx
),

MobX_mapping2_a AS(
SELECT
*,
Data1_admob_final.Ad_unit as Ad_unit1,
Data1_admob_final.Mediation_group AS Mediation_group1,
Data1_admob_final.Ad_source_instance AS Ad_source_instance1,
Data1_admob_final.Ad_source AS Ad_source_1,
CASE 
  WHEN Data1_admob_final.Ad_source='Custom Event'
     THEN Mapping.Ad_source
  ELSE Data1_admob_final.Ad_source
END AS Ad_NetworkSSP
FROM Mobx_mapping1 AS Data1_admob_final
LEFT JOIN  Mapping
ON Data1_admob_final.Ad_unit = Mapping.ad_unit and Data1_admob_final.Mediation_group=Mapping.Mediation_group and Data1_admob_final.Layer=Mapping.Ad_source_instance
),
MobX_mapping2_b AS(
 SELECT
 mxa.App AS App,
 mxa.Platform as Platform,
 mxa.Format as Format,
 mxa.Ad_unit1 as Ad_unit,
 mxa.Mediation_group1 as Mediation_group,
 mxa.Ad_source_1 as Ad_source,
 mxa.Ad_source_instance1 as Ad_source_instance,
 mxa.Date as date,
 mxa.Est__earnings__USD_ as rev,
 mxa.Observed_eCPM__USD_ as CPM,
 mxa.Requests as AR,
 mxa.Match_rate as Match_rate,
 mxa.Matched_requests as Matched_requests,
 mxa.Show_rate AS Show_rate,
 mxa.Impressions as Impressions,
 mxa.CTR as CTR,
 mxa.Clicks as Clicks,
 mxa.Ad_NetworkSSP as Ad_NetworkSSP,
 mxa.Layer as Layer
 FROM MobX_mapping2_a as mxa
),

MobX_mapping2_c AS( 
SELECT
 Data1_AdMob_donemap.App AS App,
 Data1_AdMob_donemap.Platform as Platform,
 Data1_AdMob_donemap.Format as Format,
 Data1_AdMob_donemap.Ad_unit as Ad_unit,
 Data1_AdMob_donemap.Mediation_group as Mediation_group,
 Data1_AdMob_donemap.Ad_source as Ad_source,
 Data1_AdMob_donemap.Ad_source_instance as Ad_source_instance,
 Data1_AdMob_donemap.Date as date,
 Data1_AdMob_donemap.Rev as rev,
 Data1_AdMob_donemap.CPM as CPM,
 Data1_AdMob_donemap.AR as AR,
 Data1_AdMob_donemap.Match_rate as Match_rate,
 Data1_AdMob_donemap.Matched_requests as Matched_requests,
 Data1_AdMob_donemap.Show_rate AS Show_rate,
 Data1_AdMob_donemap.Impressions as Impressions,
 Data1_AdMob_donemap.CTR as CTR,
 Data1_AdMob_donemap.Clicks as Clicks,
 Data1_AdMob_donemap.Ad_NetworkSSP as Ad_NetworkSSP,
 Data1_AdMob_donemap.Layer as Layer,
 CASE
 WHEN regexp_contains(Data1_AdMob_donemap.Ad_NetworkSSP, 'GAM|ADX') 
 THEN Mapping.GAM_Ad_unit
 ELSE NULL
 END AS GAM_Adx_AdUnit 
FROM MobX_mapping2_b as Data1_AdMob_donemap
LEFT JOIN  Mapping
ON Data1_AdMob_donemap.Ad_unit= Mapping.ad_unit and Data1_AdMob_donemap.Mediation_group=Mapping.Mediation_group and Data1_AdMob_donemap.Layer=Mapping.Ad_source_instance and Data1_AdMob_donemap.Ad_NetworkSSP=Mapping.Ad_source
),
Data1_AdMob AS(
SELECT
 Data1_AdMob_All.App AS App,
 Data1_AdMob_All.Platform as Platform,
 Data1_AdMob_All.Format as Format,
 Data1_AdMob_All.Ad_unit as Ad_unit,
 Data1_AdMob_All.Mediation_group as Mediation_group,
 Data1_AdMob_All.Ad_source as Ad_source,
 Data1_AdMob_All.Ad_source_instance as Ad_source_instance,
 Data1_AdMob_All.Date as date,
 Data1_AdMob_All.Rev as rev,
 Data1_AdMob_All.CPM as CPM,
 Data1_AdMob_All.AR as AR,
 Data1_AdMob_All.Match_rate as Match_rate,
 Data1_AdMob_All.Matched_requests as Matched_requests,
 Data1_AdMob_All.Show_rate AS Show_rate,
 Data1_AdMob_All.Impressions as Impressions,
 Data1_AdMob_All.CTR as CTR,
 Data1_AdMob_All.Clicks as Clicks,
 Data1_AdMob_All.Ad_NetworkSSP as Ad_NetworkSSP,
 Data1_AdMob_All.Layer as Layer,
 Data1_AdMob_All.GAM_Adx_AdUnit,
 
CASE 
  WHEN GAM_Adx_AdUnit IS NOT NULL
  THEN GAM_sum.GAM_Revenue
  ELSE Data1_AdMob_All.Rev
  END AS GAM_Rev,
CASE 
  WHEN GAM_Adx_AdUnit IS NOT NULL
  THEN GAM_sum.GAM_AR
  ELSE Data1_AdMob_All.AR
  END AS GAM_AR,
CASE 
  WHEN GAM_Adx_AdUnit IS NOT NULL 
  THEN GAM_sum.GAM_MR
  ELSE Data1_AdMob_All.Matched_requests
  END AS GAM_MR,
CASE 
  WHEN GAM_Adx_AdUnit IS NOT NULL 
  THEN GAM_sum.GAM_Impressions
  ELSE Data1_AdMob_All.Impressions
  END AS GAM_Impressions,
CASE 
  WHEN GAM_Adx_AdUnit IS NOT NULL 
  THEN GAM_sum.GAM_Clicks
  ELSE Data1_AdMob_All.Clicks
  END AS GAM_Clicks,

FROM MobX_mapping2_c Data1_AdMob_All
LEFT JOIN  Mapping
ON Data1_AdMob_All.Ad_unit= Mapping.ad_unit and Data1_AdMob_All.Mediation_group=Mapping.Mediation_group and Data1_AdMob_All.Layer=Mapping.Ad_source_instance and Data1_AdMob_All.Ad_NetworkSSP=Mapping.Ad_source

LEFT JOIN 
(
SELECT
GAM.AdX_adunit as ad_unit,
GAM.Date as date,
SUM(GAM.Revenue) as GAM_Revenue,
SUM(GAM.AR) AS GAM_AR,
SUM(GAM.Total_Code_Served_Count) AS GAM_MR,
SUM(GAM.Impressions) as GAM_Impressions,
SUM(GAM.Clicks) as GAM_Clicks
FROM GAM
WHERE GAM.AdX_adunit IS NOT NULL
GROUP BY 1,2
) GAM_sum
ON  Data1_AdMob_All.GAM_Adx_AdUnit=GAM_sum.ad_unit AND Data1_AdMob_All.Date=GAM_sum.Date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,GAM_Revenue,GAM_AR,GAM_MR,GAM_Impressions,GAM_Clicks
)

SELECT 
AdMob2.App as App,
AdMob2.Platform as Platform,
AdMob2.Format as Format,
AdMob2.Ad_unit as Ad_unit,
AdMob2.Mediation_group as Mediation_group ,
AdMob2.Date,
Data1_AdMob_mediationmap.Rev as rev,
AdMob2.Requests as UniqueAR,
Data1_AdMob_mediationmap.Impressions as Impressions,
Data1_AdMob_mediationmap.MR as MR,
SAFE_DIVIDE(Data1_AdMob_mediationmap.Rev,AdMob2.Requests) *1000 as AR_CPM,
SAFE_DIVIDE(Data1_AdMob_mediationmap.Rev,Data1_AdMob_mediationmap.Impressions) *1000 as CPM,
SAFE_DIVIDE(Data1_AdMob_mediationmap.Impressions,AdMob2.Requests) as Fill_rate,
SAFE_DIVIDE(Data1_AdMob_mediationmap.MR,AdMob2.Requests) as Match_rate,
SAFE_DIVIDE(Data1_AdMob_mediationmap.Impressions,Data1_AdMob_mediationmap.MR) as Show_rate,
Data1_AdMob_mediationmap.clicks as clicks, 
SAFE_DIVIDE(Data1_AdMob_mediationmap.clicks,Data1_AdMob_mediationmap.Impressions) as CTR
FROM `anymanager-playground.GogoLook_db.Data2_AdMob` AdMob2
LEFT JOIN
( 
SELECT
Data1_AdMob.ad_unit as Ad_unit,
Data1_AdMob.Mediation_group as Mediation_group,
Data1_AdMob.date as Date,
SUM(Data1_AdMob.GAM_Rev) as Rev,
SUM(Data1_AdMob.GAM_MR) as MR,
SUM(Data1_AdMob.GAM_Impressions) as Impressions,
SUM(Data1_AdMob.GAM_Clicks) as clicks
FROM Data1_AdMob 
Group by 1,2,3
) as Data1_AdMob_mediationmap
ON Data1_AdMob_mediationmap.ad_unit=AdMob2.Ad_unit AND Data1_AdMob_mediationmap.Mediation_group=AdMob2.Mediation_group AND Data1_AdMob_mediationmap.date=AdMob2.Date
