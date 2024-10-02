with CTE1 as (select ad_date,
       adset_name,
       campaign_name,
       'Facebook ads' as media_source,
       coalesce(spend,0) as spend, 
       coalesce(impressions,0) as impressions,
       coalesce(reach,0) as reach,
       coalesce(clicks,0) as clicks,
       coalesce(leads,0) as leads,
       coalesce(value,0) as value,
       case 
       	 when substring(url_parameters FROM 'utm_campaign=([^&]+)') = 'nan' then null
        WHEN substring(url_parameters FROM 'utm_campaign=([^&]+)') IS NOT NULL THEN lower(substring(url_parameters FROM 'utm_campaign=([^&]+)'))
       end as utm_campaign
from facebook_ads_basic_daily
inner join facebook_adset on facebook_adset.adset_id=facebook_ads_basic_daily.adset_id
inner join facebook_campaign on facebook_campaign.campaign_id =facebook_ads_basic_daily.campaign_id
union all 
select ad_date,
       adset_name,
       campaign_name,
       'Google ads' as media_source,
       coalesce(spend,0) as spend, 
       coalesce(impressions,0) as impressions,
       coalesce(reach,0) as reach,
       coalesce(clicks,0) as clicks,
       coalesce(leads,0) as leads,
       coalesce(value,0) as value,
       case
        when substring(url_parameters FROM 'utm_campaign=([^&]+)') = 'nan' then null
        WHEN substring(url_parameters FROM 'utm_campaign=([^&]+)') IS NOT NULL THEN lower(substring(url_parameters FROM 'utm_campaign=([^&]+)'))
       end as utm_campaign
from google_ads_basic_daily),
CTE2 as (select 
       date_trunc('month', ad_date) AS ad_month,
       utm_campaign,
       sum(spend) as total_spend,
       sum(impressions) as total_impressions,
       sum(clicks) as total_clicks,
       sum(value) as total_value,
        CASE
        WHEN sum(impressions) <>  0 THEN sum(clicks)::numeric/sum(impressions)
        ELSE null
    END AS CTR,
    case
    	when sum(clicks)<>0 then sum(spend)/sum(clicks)
    	else null
    end as CPC,
    case 
	    when sum(impressions)<>0 then  1000*sum(spend)/sum(impressions)
	    else null
    end as CPM,
    case
	    when sum(spend)<>0 then  (sum (value)-sum (spend))::numeric/sum(spend) 
    	else null
    end as ROMI
from CTE1
group by ad_month,
         utm_campaign),  
 CTE3 as (select ad_month,
                 utm_campaign,
                 total_spend,
                 total_impressions,
                 total_clicks,
                 total_value,
                 CTR,
                 lag(CTR) over (partition by utm_campaign order by ad_month desc) as previous_CTR,
                 CPC,
                 lag(CPC) over (partition by utm_campaign order by ad_month desc) as previous_CPC,
                 CPM,
                 lag(CPM) over (partition by utm_campaign order by ad_month desc) as previous_CPM,
                 RoMI,
                 lag(ROMI) over (partition by utm_campaign order by ad_month desc) as previous_ROMI
          from CTE2
       )
select ad_month,
       utm_campaign,
       case 
		when previous_CPM > 0 then (CPM - previous_CPM ::numeric )/ previous_CPM
		when previous_CPM = 0 and CPM > 0 then 1
	end as difference_CPM,
	case 
		when previous_CPC > 0 then (CPC - previous_CPC ::numeric )/ previous_CPC
		when previous_CPC = 0 and CPC > 0 then 1
	end as difference_CPC,
	case 
		when previous_CTR > 0 then (CTR - previous_CTR ::numeric) / previous_CTR
		when previous_CTR = 0 and CTR > 0 then 1
	end as difference_CTR,
	case 
		when previous_ROMI > 0 then (ROMI - previous_ROMI ::numeric )/previous_ROMI
		when previous_ROMI = 0 and ROMI > 0 then 1
	end as difference_ROMI
from CTE3
order by utm_campaign,
         ad_month;





