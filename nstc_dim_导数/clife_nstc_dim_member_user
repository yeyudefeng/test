insert overwrite table db_dw_nstc.clife_nstc_dim_member_user partition(part_date)
select   
     a.member_id                      -- '会员id' 
    ,a.member_phone                   -- '手机号' 
    ,a.member_name                    -- '昵称' 
    ,a.member_avator                  -- '头像' 
    ,a.intro_user                     -- '推荐人' 
    ,a.`level`                        -- '身份0-个人,1-企业,2-经纪人,10-未选择' 
    ,a.unionid                        -- '微信unionid' 
    ,a.openid                         -- '微信openId' 
    ,a.wx_name                        -- '微信昵称' 
    ,a.ali_name                       -- '阿里名字' 
    ,a.ali_account                    -- '阿里手机号' 
    ,b.icbc_id                        -- '工行认证id' 
    ,b.vendor_short_name              -- '企业用户简称' 
    ,b.vendor_email                   -- '企业用户邮箱' 
    ,b.province                       -- '所在省 code例130000' 
    ,b.city                           -- '所在市 code例130100' 
    ,b.county                         -- '所在区县code例130102' 
    ,b.address                        -- '详细地址' 
    ,b.postcode                       -- '邮政编码' 
    ,b.vendor_type                    -- '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户' 
    ,b.cert_type                      -- '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证' 
    ,b.cert_no                        -- '企业用户注册证件号码，' 
    ,b.cert_pic                       -- '企业用户注册证件图片' 
    ,b.cert_validityl_str             -- '企业用户注册证件有 效期' 
    ,a.create_at                      -- '注册时间' 
    ,a.update_at                      -- '更新时间' 
    ,a.part_date
from (
select 
     member_id                      -- '会员id' 
    ,member_phone                   -- '手机号' 
    ,member_name                    -- '昵称' 
    ,member_avator                  -- '头像' 
    ,intro_user                     -- '推荐人' 
    ,`level`                        -- '身份0-个人,1-企业,2-经纪人,10-未选择' 
    ,unionid                        -- '微信unionid' 
    ,openid                         -- '微信openId' 
    ,wx_name                        -- '微信昵称' 
    ,ali_name                       -- '阿里名字' 
    ,ali_account                    -- '阿里手机号' 
    ,create_at                      -- '注册时间' 
    ,update_at                      -- '更新时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_member_user
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 and disabled = 0
) a 
left join db_ods_nstc.clife_nstc_ods_member_icbc b 
on a.part_date = b.part_date and a.member_id = b.member_id  
