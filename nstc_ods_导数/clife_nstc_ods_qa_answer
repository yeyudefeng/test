select 
     answer_id                      -- 答案id
    ,question_id                    -- 问题id
    ,member_id                      -- 回答人id
    ,answer_content                 -- 回答内容
    ,`status`                       -- 显示状态0-显示,1-隐藏
    ,like_num                       -- 回复点赞数
    ,create_at                      -- 回答时间
    ,del_flag                       -- 删除标记0-正常
from guomiaozhishu.tb_qa_answer
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')
