CREATE TABLE if not exists dws1.dws_fund_performance_mi_edadj_tmp(
  `fund_id` varchar(10) COMMENT '����ID', 
  `nav_date` date COMMENT '��������', 
  `adjusted_nav` decimal(20,8) COMMENT '��Ȩ��ֵ'
  )
COMMENT '��ĩ����ĩ����ĩ����Ȩ��ֵ��ʱ��'
PARTITIONED BY ( 
  `adjusted_nav_mark` string COMMENT 'ʱ��ָ������')
STORED AS ORC
; 

--�ѽ�������ĩ���ӽ���ĩ���Ļ���Ȩ��ֵ���뵽��ʱ�б�
insert overwrite table dws1.dws_fund_performance_mi_edadj_tmp PARTITION (adjusted_nav_mark)
select
--t.*,
--t1.*,
 nvl(t.fund_id, t1.fund_id) as fund_id,
 case
   when t.day_mins <= t1.day_mins then
    t.nav_date
   when t.day_mins > t1.day_mins then
    t1.nav_date
   else
    nvl(t.nav_date, t1.nav_date)
 end as nav_date,
 case
   when t.day_mins <= t1.day_mins then
    t.adjusted_nav
   when t.day_mins > t1.day_mins then
    t1.adjusted_nav
   else
    nvl(t.adjusted_nav, t1.adjusted_nav)
 end as adjusted_nav,
 'last_month_endday_fund' as adjusted_nav_mark
  from (select t.*
          from (select t.*,
                       row_number() over(partition by t.fund_id order by day_mins) rn
                  from (select t.*
                          from (select t.fund_id,
                                       t1.calendar_id,
                                       t1.is_trd_day,
                                       t.nav_date,
                                       t.adjusted_nav,
                                       dense_rank() over(order by t1.calendar_id desc) as day_mins
                                  from dwh1.ods_edc_fdm_etl_calendar_df t1
                                  full join (select t.*
                                              from dwd1.dwd_fund_nav_di t
                                             where dt = '${yyyyMMdd}') t
                                    on t.nav_date =
                                       to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                            'yyyyMMdd'),
                                                             'yyyy-MM-dd'))
                                 where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                            'yyyyMMdd'),
                                                             'yyyy-MM-dd')) <=
                                       last_day(add_months(CURRENT_DATE, -1)) --������ĩȡ���һ�쾻ֵ
                                   and t1.is_trd_day = 1
                                 order by calendar_id desc) t
                         where t.day_mins <= 5) t) t
         where t.rn = 1
         order by t.calendar_id desc) t
  full join (select t.*
               from (select t.*,
                            row_number() over(partition by t.fund_id order by day_mins) rn
                       from (select t.*
                               from (select t.fund_id,
                                            t1.calendar_id,
                                            t1.is_trd_day,
                                            t.nav_date,
                                            t.adjusted_nav,
                                            dense_rank() over(order by t1.calendar_id asc) as day_mins
                                       from dwh1.ods_edc_fdm_etl_calendar_df t1
                                       full join (select t.*
                                                   from dwd1.dwd_fund_nav_di t
                                                  where dt = '${yyyyMMdd}') t
                                         on t.nav_date =
                                            to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                 'yyyyMMdd'),
                                                                  'yyyy-MM-dd'))
                                      where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                 'yyyyMMdd'),
                                                                  'yyyy-MM-dd')) >=
                                            last_day(add_months(CURRENT_DATE,
                                                                -1)) --������ĩȡ���һ�쾻ֵ
                                        and t1.is_trd_day = 1
                                      order by calendar_id asc) t
                              where t.day_mins <= 5) t) t
              where t.rn = 1
              order by t.calendar_id desc) t1
    on t.fund_id = t1.fund_id;

--�ѽ�������ĩ���ӽ���ĩ����ָ����ֵ���뵽��ʱ�б�
insert overwrite table dws1.dws_fund_performance_mi_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when t.day_mins <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when t.day_mins <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'last_month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by day_mins) rn
                    from (select t.*
                            from (select t.fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.nav_date,
                                         t.adjusted_nav,
                                         dense_rank() over(order by t1.calendar_id desc) as day_mins
                                    from dwh1.ods_edc_fdm_etl_calendar_df t1
                                    full join (select t.*
                                                from dwd1.dwd_index_nav_di t
                                               where dt = '${yyyyMMdd}') t
                                      on t.nav_date =
                                         to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                              'yyyyMMdd'),
                                                               'yyyy-MM-dd'))
                                   where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                              'yyyyMMdd'),
                                                               'yyyy-MM-dd')) <=
                                         last_day(add_months(CURRENT_DATE, -1)) --������ĩȡ���һ�쾻ֵ
                                     and t1.is_trd_day = 1
                                   order by calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from dwh1.ods_edc_fdm_etl_calendar_df t1
                                         full join (select t.*
                                                     from dwd1.dwd_index_nav_di t
                                                    where dt = '${yyyyMMdd}') t
                                           on t.nav_date =
                                              to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                   'yyyyMMdd'),
                                                                    'yyyy-MM-dd'))
                                        where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                   'yyyyMMdd'),
                                                                    'yyyy-MM-dd')) >=
                                              last_day(add_months(CURRENT_DATE,
                                                                  -1)) --������ĩȡ���һ�쾻ֵ
                                          and t1.is_trd_day = 1
                                        order by calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

CREATE TABLE if not exists dws1.dws_fund_performance_mi_tmp(fund_id
                                                            varchar(10)
                                                            COMMENT '����ID',
                                                            end_date
                                                            varchar(10)
                                                            COMMENT '��������',
                                                            price_date date
                                                            COMMENT '��ֵ����',
                                                            adjusted_nav
                                                            decimal(20, 8)
                                                            COMMENT '��Ȩ��ֵ',
                                                            ret_1m
                                                            decimal(20, 8)
                                                            COMMENT '��һ������',
                                                            ret_1m_aN
                                                            decimal(20, 8)
                                                            COMMENT
                                                            '��һ���껪����',
                                                            ret_index_mark
                                                            varchar(50)
                                                            COMMENT
                                                            'ָ��mark') COMMENT '��ֹ��ĩ�������' STORED AS ORC;

--��������һ�������棬�껯����
insert overwrite table dws1.dws_fund_performance_mi_tmp
  select
  --t.*,t1.*
   nvl(t.fund_id, t1.fund_id) as fund_id,
   substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'), 1, 7) as end_date, ---��ĩ
   t1.nav_date as price_date, ---��ĩ����
   t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
   (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_1m, --��һ������
   t.nav_date,
   t.adjusted_nav,
   POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
         365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_1m_a, --��һ���껯����
   'ret_1m_a_fund' as ret_index_mark
    from (select
          --t.*,
          --t1.*,
           nvl(t.fund_id, t1.fund_id) as fund_id,
           case
             when t.day_mins <= t1.day_mins then
              t.nav_date
             when t.day_mins > t1.day_mins then
              t1.nav_date
             else
              nvl(t.nav_date, t1.nav_date)
           end as nav_date,
           case
             when t.day_mins <= t1.day_mins then
              t.adjusted_nav
             when t.day_mins > t1.day_mins then
              t1.adjusted_nav
             else
              nvl(t.adjusted_nav, t1.adjusted_nav)
           end as adjusted_nav,
           'last_month_endday_fund' as adjusted_nav_mark
            from (select t.*
                    from (select t.*,
                                 row_number() over(partition by t.fund_id order by day_mins) rn
                            from (select t.*
                                    from (select t.fund_id,
                                                 t1.calendar_id,
                                                 t1.is_trd_day,
                                                 t.nav_date,
                                                 t.adjusted_nav,
                                                 dense_rank() over(order by t1.calendar_id desc) as day_mins
                                            from dwh1.ods_edc_fdm_etl_calendar_df t1
                                            full join (select t.*
                                                        from dwd1.dwd_fund_nav_di t
                                                       where dt = '${yyyyMMdd}') t
                                              on t.nav_date =
                                                 to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                      'yyyyMMdd'),
                                                                       'yyyy-MM-dd'))
                                           where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                      'yyyyMMdd'),
                                                                       'yyyy-MM-dd')) <=
                                                 last_day(add_months(CURRENT_DATE,
                                                                     -2)) --С���³�ȡ���һ�쾻ֵ
                                             and t1.is_trd_day = 1
                                           order by calendar_id desc) t
                                   where t.day_mins <= 5) t) t
                   where t.rn = 1
                   order by t.calendar_id desc) t
            full join (select t.*
                        from (select t.*,
                                     row_number() over(partition by t.fund_id order by day_mins) rn
                                from (select t.*
                                        from (select t.fund_id,
                                                     t1.calendar_id,
                                                     t1.is_trd_day,
                                                     t.nav_date,
                                                     t.adjusted_nav,
                                                     dense_rank() over(order by t1.calendar_id asc) as day_mins
                                                from dwh1.ods_edc_fdm_etl_calendar_df t1
                                                full join (select t.*
                                                            from dwd1.dwd_fund_nav_di t
                                                           where dt =
                                                                 '${yyyyMMdd}') t
                                                  on t.nav_date =
                                                     to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                          'yyyyMMdd'),
                                                                           'yyyy-MM-dd'))
                                               where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                          'yyyyMMdd'),
                                                                           'yyyy-MM-dd')) >=
                                                     last_day(add_months(CURRENT_DATE,
                                                                         -2)) --�����³�ȡ���һ�쾻ֵ
                                                 and t1.is_trd_day = 1
                                               order by calendar_id asc) t
                                       where t.day_mins <= 5) t) t
                       where t.rn = 1
                       order by t.calendar_id desc) t1
              on t.fund_id = t1.fund_id) t
    full join (select t1.*
                 from test_1022 t1
                where t1.adjusted_nav_mark = 'last_month_endday_fund') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ����һ�������棬�껯����
insert overwrite table dws1.dws_fund_performance_mi_tmp
  select
  --t.*,t1.*
   nvl(t.fund_id, t1.fund_id) as fund_id,
   substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'), 1, 7) as end_date, ---��ĩ
   t1.nav_date as price_date, ---��ĩ����
   t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
   (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_1m, --��һ������
   t.nav_date,
   t.adjusted_nav,
   POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
         365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_1m_a, --��һ���껯����
   'ret_1m_a_index' as ret_index_mark
    from (select
          --t.*,
          --t1.*,
           nvl(t.fund_id, t1.fund_id) as fund_id,
           case
             when t.day_mins <= t1.day_mins then
              t.nav_date
             when t.day_mins > t1.day_mins then
              t1.nav_date
             else
              nvl(t.nav_date, t1.nav_date)
           end as nav_date,
           case
             when t.day_mins <= t1.day_mins then
              t.adjusted_nav
             when t.day_mins > t1.day_mins then
              t1.adjusted_nav
             else
              nvl(t.adjusted_nav, t1.adjusted_nav)
           end as adjusted_nav,
           'last_month_endday_index' as adjusted_nav_mark
            from (select t.*
                    from (select t.*,
                                 row_number() over(partition by t.fund_id order by day_mins) rn
                            from (select t.*
                                    from (select t.fund_id,
                                                 t1.calendar_id,
                                                 t1.is_trd_day,
                                                 t.nav_date,
                                                 t.adjusted_nav,
                                                 dense_rank() over(order by t1.calendar_id desc) as day_mins
                                            from dwh1.ods_edc_fdm_etl_calendar_df t1
                                            full join (select t.*
                                                        from dwd1.dwd_index_nav_di t
                                                       where dt = '${yyyyMMdd}') t
                                              on t.nav_date =
                                                 to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                      'yyyyMMdd'),
                                                                       'yyyy-MM-dd'))
                                           where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                      'yyyyMMdd'),
                                                                       'yyyy-MM-dd')) <=
                                                 last_day(add_months(CURRENT_DATE,
                                                                     -2)) --С���³�ȡ���һ�쾻ֵ
                                             and t1.is_trd_day = 1
                                           order by calendar_id desc) t
                                   where t.day_mins <= 5) t) t
                   where t.rn = 1
                   order by t.calendar_id desc) t
            full join (select t.*
                        from (select t.*,
                                     row_number() over(partition by t.fund_id order by day_mins) rn
                                from (select t.*
                                        from (select t.fund_id,
                                                     t1.calendar_id,
                                                     t1.is_trd_day,
                                                     t.nav_date,
                                                     t.adjusted_nav,
                                                     dense_rank() over(order by t1.calendar_id asc) as day_mins
                                                from dwh1.ods_edc_fdm_etl_calendar_df t1
                                                full join (select t.*
                                                            from dwd1.dwd_index_nav_di t
                                                           where dt =
                                                                 '${yyyyMMdd}') t
                                                  on t.nav_date =
                                                     to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                          'yyyyMMdd'),
                                                                           'yyyy-MM-dd'))
                                               where to_date(from_unixtime(unix_timestamp(t1.calendar_id,
                                                                                          'yyyyMMdd'),
                                                                           'yyyy-MM-dd')) >=
                                                     last_day(add_months(CURRENT_DATE,
                                                                         -2)) --�����³�ȡ���һ�쾻ֵ
                                                 and t1.is_trd_day = 1
                                               order by calendar_id asc) t
                                       where t.day_mins <= 5) t) t
                       where t.rn = 1
                       order by t.calendar_id desc) t1
              on t.fund_id = t1.fund_id) t
    full join (select t1.*
                 from test_1022 t1
                where t1.adjusted_nav_mark = 'last_month_endday_fund') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;
