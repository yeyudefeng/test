insert overwrite table db_ads_nstc.clife_nstc_ads_deal_message_info_d partition(part_date)
select 
     member_id                                                    -- '会员id'  
    ,release_day                                                  -- '发布时间（日）'
    ,max(member_name)          as member_name                     -- '昵称'
    ,sum(purchase_cnt        ) as purchase_cnt                    -- '采购信息数量'
    ,sum(suppery_cnt         ) as suppery_cnt                     -- '供应信息数量'
    ,sum(rent_cnt            ) as rent_cnt                        -- '车找苗信息数量'
    ,sum(lease_cnt           ) as lease_cnt                       -- '苗找车信息数量'
    ,sum(purchase_release_cnt) as purchase_release_cnt            -- '采购信息发布中数量'
    ,sum(suppery_release_cnt ) as suppery_release_cnt             -- '供应信息发布中数量'
    ,sum(rent_release_cnt    ) as rent_release_cnt                -- '车找苗信息发布中数量'
    ,sum(lease_release_cnt   ) as lease_release_cnt               -- '苗找车信息发布中数量'
    ,sum(purchase_finish_cnt ) as purchase_finish_cnt             -- '采购信息已结束数量'
    ,sum(suppery_finish_cnt  ) as suppery_finish_cnt              -- '供应信息已结束数量'
    ,sum(rent_finish_cnt     ) as rent_finish_cnt                 -- '车找苗信息已结束数量'
    ,sum(lease_finish_cnt    ) as lease_finish_cnt                -- '苗找车信息已结束数量'
    ,sum(purchase_return_cnt ) as purchase_return_cnt             -- '采购信息被驳回数量'
    ,sum(suppery_return_cnt  ) as suppery_return_cnt              -- '供应信息被驳回数量'
    ,sum(rent_return_cnt     ) as rent_return_cnt                 -- '车找苗信息被驳回数量'
    ,sum(lease_return_cnt    ) as lease_return_cnt                -- '苗找车信息被驳回数量'
    ,sum(purchase_scan_num   ) as purchase_scan_num               -- '采购信息浏览量'
    ,sum(suppery_scan_num    ) as suppery_scan_num                -- '供应信息浏览量'
    ,sum(rent_scan_num       ) as rent_scan_num                   -- '车找苗信息浏览量'
    ,sum(lease_scan_num      ) as lease_scan_num                  -- '苗找车信息浏览量'
	,max(part_date)            as part_date                       -- '分区日期'
from db_dw_nstc.clife_nstc_dws_deal_message_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by member_id, release_day