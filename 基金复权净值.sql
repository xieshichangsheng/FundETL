CREATE TABLE if not exists dws1.dws_fund_performance_mi_edadj_tmp(
  `fund_id` varchar(10) COMMENT '基金ID', 
  `nav_date` date COMMENT '结束日期', 
  `adjusted_nav` decimal(20,8) COMMENT '复权净值'
  )
COMMENT '月末，周末，日末，复权净值临时表'
PARTITIONED BY ( 
  `adjusted_nav_mark` string COMMENT '时间指标类型')
STORED AS ORC
; 

--把截至到月末（接近月末）的基金复权净值插入到临时中表
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
                                       last_day(add_months(CURRENT_DATE, -1)) --大于月末取最近一天净值
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
                                                                -1)) --大于月末取最近一天净值
                                        and t1.is_trd_day = 1
                                      order by calendar_id asc) t
                              where t.day_mins <= 5) t) t
              where t.rn = 1
              order by t.calendar_id desc) t1
    on t.fund_id = t1.fund_id;

--把截至到月末（接近月末）的指数净值插入到临时中表
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
                                         last_day(add_months(CURRENT_DATE, -1)) --大于月末取最近一天净值
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
                                                                  -1)) --大于月末取最近一天净值
                                          and t1.is_trd_day = 1
                                        order by calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

CREATE TABLE if not exists dws1.dws_fund_performance_mi_tmp(fund_id
                                                            varchar(10)
                                                            COMMENT '基金ID',
                                                            end_date
                                                            varchar(10)
                                                            COMMENT '结束日期',
                                                            price_date date
                                                            COMMENT '净值日期',
                                                            adjusted_nav
                                                            decimal(20, 8)
                                                            COMMENT '复权净值',
                                                            ret_1m
                                                            decimal(20, 8)
                                                            COMMENT '近一月收益',
                                                            ret_1m_aN
                                                            decimal(20, 8)
                                                            COMMENT
                                                            '近一月年华收益',
                                                            ret_index_mark
                                                            varchar(50)
                                                            COMMENT
                                                            '指标mark') COMMENT '截止月末的收益表' STORED AS ORC;

--插入基金近一个月收益，年化收益
insert overwrite table dws1.dws_fund_performance_mi_tmp
  select
  --t.*,t1.*
   nvl(t.fund_id, t1.fund_id) as fund_id,
   substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'), 1, 7) as end_date, ---月末
   t1.nav_date as price_date, ---月末日期
   t1.adjusted_nav as adjusted_nav, --月末净值
   (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_1m, --近一月收益
   t.nav_date,
   t.adjusted_nav,
   POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
         365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_1m_a, --近一月年化收益
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
                                                                     -2)) --小于月初取最近一天净值
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
                                                                         -2)) --大于月初取最近一天净值
                                                 and t1.is_trd_day = 1
                                               order by calendar_id asc) t
                                       where t.day_mins <= 5) t) t
                       where t.rn = 1
                       order by t.calendar_id desc) t1
              on t.fund_id = t1.fund_id) t
    full join (select t1.*
                 from test_1022 t1
                where t1.adjusted_nav_mark = 'last_month_endday_fund') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近一个月收益，年化收益
insert overwrite table dws1.dws_fund_performance_mi_tmp
  select
  --t.*,t1.*
   nvl(t.fund_id, t1.fund_id) as fund_id,
   substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'), 1, 7) as end_date, ---月末
   t1.nav_date as price_date, ---月末日期
   t1.adjusted_nav as adjusted_nav, --月末净值
   (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_1m, --近一月收益
   t.nav_date,
   t.adjusted_nav,
   POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
         365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_1m_a, --近一月年化收益
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
                                                                     -2)) --小于月初取最近一天净值
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
                                                                         -2)) --大于月初取最近一天净值
                                                 and t1.is_trd_day = 1
                                               order by calendar_id asc) t
                                       where t.day_mins <= 5) t) t
                       where t.rn = 1
                       order by t.calendar_id desc) t1
              on t.fund_id = t1.fund_id) t
    full join (select t1.*
                 from test_1022 t1
                where t1.adjusted_nav_mark = 'last_month_endday_fund') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;
