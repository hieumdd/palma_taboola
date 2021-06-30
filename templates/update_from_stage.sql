CREATE
OR REPLACE TABLE Palma.TopCampaignContent AS
SELECT
    *
EXCEPT
    (row_num)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY item,
                campaign,
                content_provider,
                start_date,
                end_date
                ORDER BY
                    last_used_rawdata_update_time DESC
            ) AS row_num
        FROM
            Palma._stage_TopCampaignContent
    )
WHERE
    row_num = 1
