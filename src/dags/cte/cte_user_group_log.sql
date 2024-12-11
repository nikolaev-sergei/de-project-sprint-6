with user_group_log as 
      (select gl.hk_group_id as hk_group_id,
              count(distinct lglu.hk_user_id) as cnt_added_users
       from (select g.hk_group_id,
                    lglg.hk_group_log_id
             from (select hg.hk_group_id
                   from KK91YANDEXRU__DWH.h_groups hg
                   order by hg.registration_dt
                   limit 10) as g
             left join KK91YANDEXRU__DWH.l_group_logs_groups lglg on g.hk_group_id = lglg.hk_group_id) as gl
       left join KK91YANDEXRU__DWH.l_group_logs_users lglu on gl.hk_group_log_id = lglu.hk_group_log_id
       left join KK91YANDEXRU__DWH.s_auth_history sah on gl.hk_group_log_id = sah.hk_group_log_id
       where sah.event = 'add'
       group by gl.hk_group_id)
select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;