with user_group_messages as 
       (select hg.hk_group_id as hk_group_id,
               coalesce(cnt.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages
        from STV2024111147__DWH.h_groups hg
        left join (select lgd.hk_group_id as hk_group_id,
                          count(distinct lumf.hk_user_id) as cnt_users_in_group_with_messages
                   from STV2024111147__DWH.l_groups_dialogs lgd
                   left join STV2024111147__DWH.l_user_message_from lumf on lgd.hk_message_id = lumf.hk_message_id
                   group by lgd.hk_group_id) cnt on hg.hk_group_id = cnt.hk_group_id),
     user_group_log as 
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
select ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       (ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users * 100)::numeric(5,2) as group_conversion
from user_group_log ugl
left join user_group_messages ugm on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;