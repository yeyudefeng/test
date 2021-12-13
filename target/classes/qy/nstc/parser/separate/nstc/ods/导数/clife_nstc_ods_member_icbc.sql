select 
     icbc_id                        -- 工行认证id
    ,member_id                      -- 会员id
    ,vendor_short_name              -- 企业用户简称
    ,vendor_name                    -- 企业用户名称，开通 E企付担保支付时必 输
    ,vendor_phone                   -- 企业用户联系电话(会员手机号,带过来不可修改)
    ,vendor_email                   -- 企业用户邮箱
    ,province                       -- 所在省 code例130000
    ,city                           -- 所在市 code例130100
    ,county                         -- 所在区县code例130102
    ,address                        -- 详细地址
    ,postcode                       -- 邮政编码
    ,vendor_type                    -- 企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户
    ,cert_type                      -- 企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证
    ,cert_no                        -- 企业用户注册证件号码，
    ,cert_pic                       -- 企业用户注册证件图片
    ,cert_validityl_str             -- 企业用户注册证件有 效期
    ,operator_name                  -- 企业用户联系人姓名
    ,operator_mobile                -- 企业用户联系人手机
    ,operator_email                 -- 企业用户联系人邮箱
    ,operatorId_no                  -- 企业用户联系人身份 证号
    ,`status`                       -- 状态0-未提交,1-审核中,2-审核成功,3-审核失败,4-提交失败
    ,reason                         -- 失败原因
from guomiaozhishu.tb_member_icbc