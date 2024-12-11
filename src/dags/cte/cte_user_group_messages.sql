with user_group_messages as 
       (select hg.hk_group_id as hk_group_id,
               coalesce(cnt.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages
        from STV2024111147__DWH.h_groups hg
        left join (select lgd.hk_group_id as hk_group_id,
                          count(distinct lumf.hk_user_id) as cnt_users_in_group_with_messages
                   from STV2024111147__DWH.l_groups_dialogs lgd
                   left join STV2024111147__DWH.l_user_message_from lumf on lgd.hk_message_id = lumf.hk_message_id
                   group by lgd.hk_group_id) cnt on hg.hk_group_id = cnt.hk_group_id)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;