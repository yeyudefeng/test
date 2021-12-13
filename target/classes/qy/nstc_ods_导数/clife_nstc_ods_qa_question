select 
     question_id                    -- 问题id
    ,member_id                      -- 会员id
    ,question_content               -- 问题描述
    ,question_img                   -- 图片链接
    ,`status`                       -- 显示状态0-显示,1-隐藏
    ,like_num                       -- 点赞数
    ,create_at                      -- 发布时间
    ,del_flag                       -- 删除标记0-正常
from guomiaozhishu.tb_qa_question
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')