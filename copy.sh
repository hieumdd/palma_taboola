for i in $(bq ls -n 10000 "sugatan-290314:Palma" | awk '{print $1}' | tail +3); do
    echo $i
    bq cp --force=true --headless=true "sugatan-290314:Palma.$i" "palma-331310:Palma.$i"
done

for i in Mappings Taboola_country_campaign Taboola_country_campaign_master campaign_creative_data campaign_creative_data_view_leads campaign_site_day_data channel_overview_day conversion_data creative_data_outbrain master_campaign_vew_visit master_view outbrain_browser_campaign_master outbrain_browser_campaign_metircs outbrain_country_campaign outbrain_country_campaign_master outbrain_creative_data_master outbrain_creative_master_data outbrain_hourly outbrain_hourly_master outbrain_hourly_master_v outbrain_hourly_test_dnu outbrain_os_campaign_master outbrain_os_campaign_metircs outbrain_publisher_data outbrain_publisher_master_data outbrain_publisher_master_data_view_lead_report outbrain_region_country_view outbrain_section_data outbrain_section_master_data_v spend_hourly_view_outbrain taboola_CampaignSummaryRegion taboola_browser_campaign taboola_browser_campaign_master taboola_campaign_creative_master taboola_campaign_site_master taboola_final taboola_hourly taboola_hourly_dnu taboola_hourly_master taboola_hourly_master_v taboola_os_family_campaign taboola_os_family_campaign_master taboola_osversion_campaign taboola_osversion_campaign_master taboola_view_metric_report taboola_view_metric_report_creative taboola_view_metric_report_site; do
    query=$(bq show --format=prettyjson sugatan-290314:Palma.$i | jq -r '.view.query' | sed -e 's/sugatan-290314.//g')
    echo $i
    bq mk --view=$query --use_legacy_sql=false --force=true --project_id=palma-331310 Palma.$i
done
