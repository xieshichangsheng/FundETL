--把截至到月末（接近月末）的基金复权净值插入到临时中表
insert overwrite table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select t.fund_id,
         t.nav_date,
         t.adjusted_nav,
         'end_month_endday_fund' as adjusted_nav_mark
    from (select t.fund_id,
                 t1.calendar_id,
                 t1.is_trd_day,
                 t.nav_date,
                 t.adjusted_nav,
                 row_number() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins --取最新净值日期
            from (select t.price_date as calendar_id, 1 as is_trd_day
                    from dwd1.dwd_index_nav_di t
                   where t.index_id = 'IN00000001') t1
            full join dwd1.dwd_fund_nav_di t
              on t.nav_date = t1.calendar_id
           where t.dt = (select max(dt) from dwd1.dwd_fund_nav_di)
           order by t1.calendar_id desc) t
   where t.day_mins = 1;

--把截至到近一周（接近上周五）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select t.fund_id,
         t.nav_date,
         t.adjusted_nav,
         'early_1week_endday_fund' as adjusted_nav_mark
    from (select datediff(date_sub(next_day(CURRENT_DATE, 'FR'), 7),
                          t.nav_date) as day_mins,
                 date_sub(date_sub(next_day(CURRENT_DATE, 'FR'), 7), 5) as bf_days,
                 t.fund_id,
                 t.nav_date,
                 t.adjusted_nav,
                 ROW_NUMBER() over(PARTITION by t.fund_id order by t.nav_date desc) rn
            from dwd1.dwd_fund_nav_di t
           where t.dt = (select max(dt) from dwd1.dwd_fund_nav_di)
             and t.nav_date <= date_sub(next_day(CURRENT_DATE, 'FR'), 7)
             and t.nav_date >=
                 date_sub(date_sub(next_day(CURRENT_DATE, 'FR'), 7), 5)) t
   where rn = 1;

--把截至到近一月月初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_1month_endday_fund' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.nav_date,
                                         t.adjusted_nav,
                                         dense_rank() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -1) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_fund') t2
                                      on t.fund_id = t2.fund_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(partition by t.fund_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -1) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_fund') t2
                                           on t.fund_id = t2.fund_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近三月月初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_3month_endday_fund' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.nav_date,
                                         t.adjusted_nav,
                                         dense_rank() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -3) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_fund') t2
                                      on t.fund_id = t2.fund_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <= t2.last_month --小于三月前取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(partition by t.fund_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -3) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_fund') t2
                                           on t.fund_id = t2.fund_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >= t2.last_month --大于三月前取最近一天净值
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近六月月初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_6month_endday_fund' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.nav_date,
                                         t.adjusted_nav,
                                         dense_rank() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -6) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_fund') t2
                                      on t.fund_id = t2.fund_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <= t2.last_month --小于六月前取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(partition by t.fund_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -6) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_fund') t2
                                           on t.fund_id = t2.fund_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >= t2.last_month --大于六月前取最近一天净值
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近一年初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_1year_endday_fund' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.nav_date,
                                         t.adjusted_nav,
                                         dense_rank() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -12) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_fund') t2
                                      on t.fund_id = t2.fund_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <= t2.last_month --小于1年前取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(partition by t.fund_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -12) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_fund') t2
                                           on t.fund_id = t2.fund_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >= t2.last_month --大于1年前取最近一天净值
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到本月以来月初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
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
   'early_month_endday_fund' as adjusted_nav_mark
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
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -1)) --小于月初取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -1)) --大于月初取最近一天净值
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到今年以来月初（接近月初）的基金复权净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
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
   'early_year_endday_fund' as adjusted_nav_mark
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
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_fund_nav_di t
                                      on t.nav_date = t1.calendar_id
                                   where t.dt =
                                         (select max(dt)
                                            from dwd1.dwd_fund_nav_di)
                                     and t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -11)) --小于月初取最近一天净值
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.fund_id,
                                              t1.calendar_id,
                                              t.nav_date,
                                              t.adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_fund_nav_di t
                                           on t.nav_date = t1.calendar_id
                                        where t.dt =
                                              (select max(dt)
                                                 from dwd1.dwd_fund_nav_di)
                                          and t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -11)) --大于月初取最近一天净值
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到月末（接近月末）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select t.fund_id,
         t.nav_date,
         t.adjusted_nav,
         'end_month_endday_index' as adjusted_nav_mark
    from (select t.index_id as fund_id,
                 t1.calendar_id,
                 t1.is_trd_day,
                 t.price_date as nav_date,
                 t.close as adjusted_nav,
                 row_number() over(partition by t.index_id order by t1.calendar_id desc) as day_mins --取最新净值日期
            from (select t.price_date as calendar_id, 1 as is_trd_day
                    from dwd1.dwd_index_nav_di t
                   where t.index_id = 'IN00000001') t1
            full join dwd1.dwd_index_nav_di t
              on t.price_date = t1.calendar_id
           where t.index_id != 'IN00000015'
           order by t1.calendar_id desc) t
   where t.day_mins = 1
  union all
  select t.fund_id,
         t.nav_date,
         t.adjusted_nav,
         'end_month_endday_index' as adjusted_nav_mark
    from (select t.index_id as fund_id,
                 t1.calendar_id,
                 t1.is_trd_day,
                 t.price_date as nav_date,
                 t.close as adjusted_nav,
                 row_number() over(partition by t.index_id order by t1.calendar_id desc) as day_mins --取最新净值日期
            from (select t.price_date as calendar_id, 1 as is_trd_day
                    from dwd1.dwd_index_nav_di t
                   where t.index_id = 'IN00000015') t1
            full join dwd1.dwd_index_nav_di t
              on t.price_date = t1.calendar_id
           where t.index_id = 'IN00000015'
           order by t1.calendar_id desc) t
   where t.day_mins = 1;

--把截至到近一周（接近上周五）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   t.fund_id,
   t.nav_date,
   t.adjusted_nav,
   'early_1week_endday_index' as adjusted_nav_mark
    from (select datediff(date_sub(next_day(CURRENT_DATE, 'FR'), 7),
                           t.price_date) as day_mins,
                  date_sub(date_sub(next_day(CURRENT_DATE, 'FR'), 7), 5) as bf_days,
                  t.index_id as fund_id,
                  t.price_date as nav_date,
                  t. close as adjusted_nav,
                  ROW_NUMBER() over(PARTITION by t.index_id order by t.price_date desc) rn
             from dwd1.dwd_index_nav_di t
            where --t.dt='20211018' and 
            t.price_date <= date_sub(next_day(CURRENT_DATE, 'FR'), 7)
         and t.price_date >=
            date_sub(date_sub(next_day(CURRENT_DATE, 'FR'), 7), 5)) t --上周五前5天取最近一天指数收盘价
   where rn = 1;

--把截至到近一月月初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_1month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -1) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -1) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_1month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -1) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -1) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近三月月初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_3month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -3) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于三月前取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -3) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于三月前取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_3month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -3) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -3) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近六月月初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_6month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -6) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于六月前取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -6) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于六月前取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_3month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -6) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -6) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到近一年初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
  (adjusted_nav_mark)
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_1year_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -12) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于六月前取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -12) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于六月前取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
  select
  --t.*,
  --t1.*,
   nvl(t.fund_id, t1.fund_id) as fund_id,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.nav_date
     when t.day_mins > t1.day_mins then
      t1.nav_date
     else
      nvl(t.nav_date, t1.nav_date)
   end as nav_date,
   case
     when nvl(t.day_mins, 1) <= t1.day_mins then
      t.adjusted_nav
     when t.day_mins > t1.day_mins then
      t1.adjusted_nav
     else
      nvl(t.adjusted_nav, t1.adjusted_nav)
   end as adjusted_nav,
   'early_3month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by t.day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(partition by t.index_id order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                    full join (select t.fund_id,
                                                     add_months(t.nav_date, -12) as last_month
                                                from dws1.dws_fund_performance_latest_edadj_tmp t
                                               where t.adjusted_nav_mark =
                                                     'end_month_endday_index') t2
                                      on t.index_id = t2.fund_id
                                   where t1.calendar_id <= t2.last_month --小于上月取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t
                           where t.day_mins <= 5) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by t.day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(partition by t.index_id order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                         full join (select t.fund_id,
                                                          add_months(t.nav_date,
                                                                     -12) as last_month
                                                     from dws1.dws_fund_performance_latest_edadj_tmp t
                                                    where t.adjusted_nav_mark =
                                                          'end_month_endday_index') t2
                                           on t.index_id = t2.fund_id
                                        where t1.calendar_id >= t2.last_month --大于上月取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到本月以来月初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
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
   'early_month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                   where t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -1)) --小于月初取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                        where t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -1)) --大于月末取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
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
   'early_month_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                   where t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -1)) --小于月初取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                        where t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -1)) --大于月末取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--把截至到今年以来月初（接近月初）的指数净值插入到临时中表
insert into table dws1.dws_fund_performance_latest_edadj_tmp PARTITION
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
   'early_year_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000001') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                   where t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -11)) --小于月初取最近一天净值
                                     and t.index_id != 'IN00000015'
                                   order by t1.calendar_id desc) t) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000001') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                        where t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -11)) --大于月末取最近一天净值
                                          and t.index_id != 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id
  union all
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
   'early_year_endday_index' as adjusted_nav_mark
    from (select t.*
            from (select t.*,
                         row_number() over(partition by t.fund_id order by day_mins) rn
                    from (select t.*
                            from (select t.index_id as fund_id,
                                         t1.calendar_id,
                                         t1.is_trd_day,
                                         t.price_date as nav_date,
                                         t.close as adjusted_nav,
                                         dense_rank() over(order by t1.calendar_id desc) as day_mins
                                    from (select t.price_date as calendar_id,
                                                 1            as is_trd_day
                                            from dwd1.dwd_index_nav_di t
                                           where t.index_id = 'IN00000015') t1
                                    full join dwd1.dwd_index_nav_di t
                                      on t.price_date = t1.calendar_id
                                   where t1.calendar_id <=
                                         last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                  'yyyyMMdd'),
                                                                                   'yyyy-MM-dd')),
                                                             -11)) --小于月初取最近一天净值
                                     and t.index_id = 'IN00000015'
                                   order by t1.calendar_id desc) t) t) t
           where t.rn = 1
           order by t.calendar_id desc) t
    full join (select t.*
                 from (select t.*,
                              row_number() over(partition by t.fund_id order by day_mins) rn
                         from (select t.*
                                 from (select t.index_id as fund_id,
                                              t1.calendar_id,
                                              t1.is_trd_day,
                                              t.price_date as nav_date,
                                              t.close as adjusted_nav,
                                              dense_rank() over(order by t1.calendar_id asc) as day_mins
                                         from (select t.price_date as calendar_id,
                                                      1            as is_trd_day
                                                 from dwd1.dwd_index_nav_di t
                                                where t.index_id = 'IN00000015') t1
                                         full join dwd1.dwd_index_nav_di t
                                           on t.price_date = t1.calendar_id
                                        where t1.calendar_id >=
                                              last_day(add_months(to_date(from_unixtime(unix_timestamp(CURRENT_DATE,
                                                                                                       'yyyyMMdd'),
                                                                                        'yyyy-MM-dd')),
                                                                  -11)) --大于月末取最近一天净值
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--插入基金近一周收益，基准收益
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --最新周末净值
                  t.nav_date,
                  t.adjusted_nav,
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一周收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --近一周基准收益
                  null as ret_m_a,
                  null as ret_m_bmk_a,
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --近一周超额收益
                  null as as excess_ret_m_a,
                  'ret_1w_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_1week_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金近一个月收益，年化收益
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --近一月基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近一月年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --近一月基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --近一月超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --近一月年化超额收益
                  'ret_1m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_1month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金近三个月收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近三月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --近三月基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近三月年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --近三月基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --近三月超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --近三月年化超额收益
                  'ret_3m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_3month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金近六个月收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近三月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --近三月基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近三月年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --近三月基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --近三月超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --近三月年化超额收益
                  'ret_6m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_6month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金近一年收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近三月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --近三月基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近三月年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --近三月基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --近三月超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --近三月年化超额收益
                  'ret_1y_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_1year_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金本月以来收益，年化收益
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --本月以来月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --本月以来基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --本月以来年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --本月以来基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --本月以来超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --本月以来年化超额收益
                  'ret_m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入基金今年以来收益，年化收益
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---月末日期
                  t1.adjusted_nav as adjusted_nav, --月末净值
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --今年以来月收益
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --今年以来基准收益
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --今年以来年化收益
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --今年以来基准年化收益
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --今年以来超额收益
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --今年以来年化超额收益
                  'ret_y_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_year_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
      on strategy.benchmark_id = innav.index_id
     and t.nav_date = innav.price_date
    full join (select t.*, innav.close as index_nav
                 from (select t.*
                         from dws1.dws_fund_performance_latest_edadj_tmp t
                        where t.adjusted_nav_mark = 'end_month_endday_fund') t
                 left join (select t.fund_id, t.strategy_category_id_primary
                             from dwd1.dwd_fund_info_di t
                            where dt =
                                  (select max(dt) from dwd1.dwd_fund_info_di)
                              and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
                   on t.fund_id = fi.fund_id
                 left join dwh1.ods_irp_strategy_category_df strategy ---再通过dwd_fund_info_di.strategy_category_id_primary找到基准指数id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --最后关联指数nav获取指数算基准收益
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近一周收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         t.nav_date,
         t1.adjusted_nav,
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一周收益
         null as ret_m_bmk,
         null as ret_m_a,
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_1w_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_1week_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近一个月收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一月收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近一月年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_1m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_1month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近三个月收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一月收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近一月年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_3m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_3month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近六个月收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一月收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近一月年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_6m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_6month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数近一年收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --近一月收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --近一月年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_1y_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_1year_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数本月以来收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --本月以来收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --本月以来年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--插入指数今年以来收益，年化收益
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---月末
         t1.nav_date as price_date, ---月末日期
         t1.adjusted_nav as adjusted_nav, --月末净值
         t.nav_date,
         t1.adjusted_nav,
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --本月以来收益
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --本月以来年化收益
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_y_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_year_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --关联指数月末净值
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--将所有指标插入最终目标表
insert overwrite table dws1.dws_fund_performance_latest_di PARTITION
  (dt)
  select distinct null as id,
                  t.fund_id,
                  t.price_date,
                  t.adjusted_nav,
                  null as ret_1d,
                  null as ret_1d_bmk,
                  null as excess_ret_1d,
                  null as ret_1w,
                  null as ret_1w_bmk,
                  null as excess_ret_1w,
                  t.ret_1m --近一月收益
                 ,
                  t.ret_1m_bmk --近一月基准收益
                 ,
                  t.ret_1m_a --近一月年化收益
                 ,
                  t.ret_1m_bmk_a --近一月基准年化收益
                 ,
                  t.excess_ret_1m --近一月超额收益
                 ,
                  t.excess_ret_1m_a --近一月年化超额收益
                 ,
                  t.ret_3m --近三月收益
                 ,
                  t.ret_3m_bmk --近三月基准收益
                 ,
                  t.ret_3m_a --近三月年化收益
                 ,
                  t.ret_3m_bmk_a --近三月基准年化收益
                 ,
                  t.excess_ret_3m --近三月超额收益
                 ,
                  t.excess_ret_3m_a --近三月年化超额收益
                 ,
                  t.ret_6m --近六月收益
                 ,
                  t.ret_6m_bmk --近六月基准收益
                 ,
                  t.ret_6m_a --近六月年化收益
                 ,
                  t.ret_6m_bmk_a --近六月基准年化收益
                 ,
                  t.excess_ret_6m --近六月超额收益
                 ,
                  t.excess_ret_6m_a --近六月年化超额收益
                 ,
                  t.ret_1y --近一年收益
                 ,
                  t.ret_1y_bmk --近一年基准收益
                 ,
                  t.ret_1y_a --近一年年化收益
                 ,
                  t.ret_1y_bmk_a --近一年基准年化收益
                 ,
                  t.excess_ret_1y --近一年超额收益
                 ,
                  t.excess_ret_1y_a --近一年年化超额收益
                 ,
                  null as ret_2y,
                  null as ret_2y_bmk,
                  null as ret_2y_a,
                  null as ret_2y_bmk_a,
                  null as excess_ret_2y,
                  null as excess_ret_2y_a,
                  null as ret_3y,
                  null as ret_3y_bmk,
                  null as ret_3y_a,
                  null as ret_3y_bmk_a,
                  null as excess_ret_3y,
                  null as excess_ret_3y_a,
                  null as ret_5y,
                  null as ret_5y_bmk,
                  null as ret_5y_a,
                  null as ret_5y_bmk_a,
                  null as excess_ret_5y,
                  null as excess_ret_5y_a,
                  null as ret_10y,
                  null as ret_10y_bmk,
                  null as ret_10y_a,
                  null as ret_10y_bmk_a,
                  null as excess_ret_10y,
                  null as excess_ret_10y_a,
                  null as ret_ytd,
                  null as ret_ytd_bmk,
                  null as ret_ytd_a,
                  null as ret_ytd_bmk_a,
                  null as ret_qtd,
                  null as ret_qtd_bmk,
                  null as ret_qtd_a,
                  null as ret_qtd_bmk_a,
                  null as excess_ret_qtd,
                  null as excess_ret_qtd_a,
                  null as ret_mtd,
                  null as ret_mtd_bmk,
                  null as ret_mtd_a,
                  null as ret_mtd_bmk_a,
                  null as excess_ret_mtd,
                  null as excess_ret_mtd_a,
                  null as ret_incep,
                  null as ret_incep_bmk,
                  null as ret_incep_a,
                  null as ret_incep_bmk_a,
                  null as excess_ret_incep,
                  null as excess_ret_incep_a,
                  null as excess_ret_ytd,
                  null as excess_ret_ytd_a,
                  null as ret_open,
                  null as ret_open_bmk,
                  null as ret_open_a,
                  null as ret_open_bmk_a,
                  null as excess_ret_open,
                  null as excess_ret_open_a,
                  to_date(from_unixtime(unix_timestamp('20211023',
                                                       'yyyyMMdd'),
                                        'yyyy-MM-dd')) as createtime,
                  from_unixtime(unix_timestamp(current_date), 'yyyy-MM-dd') as updatetime,
                  1 as isvalid,
                  '${yyyyMMdd}' as dt
    from (select distinct coalesce(t.fund_id,
                                   t1.fund_id,
                                   t2.fund_id,
                                   t3.fund_id) as fund_id,
                          coalesce(t.price_date,
                                   t1.price_date,
                                   t2.price_date,
                                   t3.price_date) as price_date, ---月末日期
                          coalesce(t.adjusted_nav,
                                   t1.adjusted_nav,
                                   t2.adjusted_nav,
                                   t3.adjusted_nav) as adjusted_nav, --月末净值
                          t.ret_1m as ret_1m, --近一月收益
                          t.ret_1m_bmk as ret_1m_bmk, --近一月基准收益
                          t.ret_1m_a as ret_1m_a, --近一月年化收益
                          t.ret_1m_bmk_a as ret_1m_bmk_a, --近一月基准年化收益
                          t.excess_ret_1m as excess_ret_1m, --近一月超额收益
                          t.excess_ret_1m_a as excess_ret_1m_a, --近一月年化超额收益
                          t1.ret_1m as ret_3m, --近三月收益
                          t1.ret_1m_bmk as ret_3m_bmk, --近三月基准收益
                          t1.ret_1m_a as ret_3m_a, --近三月年化收益
                          t1.ret_1m_bmk_a as ret_3m_bmk_a, --近三月基准年化收益
                          t1.excess_ret_1m as excess_ret_3m, --近三月超额收益
                          t1.excess_ret_1m_a as excess_ret_3m_a, --近三月年化超额收益
                          t2.ret_1m as ret_6m, --近六月收益
                          t2.ret_1m_bmk as ret_6m_bmk, --近六月基准收益
                          t2.ret_1m_a as ret_6m_a, --近六月年化收益
                          t2.ret_1m_bmk_a as ret_6m_bmk_a, --近六月基准年化收益
                          t2.excess_ret_1m as excess_ret_6m, --近六月超额收益
                          t2.excess_ret_1m_a as excess_ret_6m_a, --近六月年化超额收益
                          t3.ret_1m as ret_1y, --近一年收益
                          t3.ret_1m_bmk as ret_1y_bmk, --近一年基准收益
                          t3.ret_1m_a as ret_1y_a, --近一年年化收益
                          t3.ret_1m_bmk_a as ret_1y_bmk_a, --近一年基准年化收益
                          t3.excess_ret_1m as excess_ret_1y, --近一年超额收益
                          t3.excess_ret_1m_a as excess_ret_1y_a --近一年年化超额收益
            from (select t.*
                    from dws1.dws_fund_performance_latest_tmp t
                   where t.ret_index_mark = 'ret_1m_a_fund') t
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_3m_a_fund') t1
              on t.fund_id = t1.fund_id
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_6m_a_fund') t2
              on t.fund_id = t2.fund_id
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_1y_a_fund') t3
              on t.fund_id = t3.fund_id
          union all
          select distinct coalesce(t.fund_id,
                                   t1.fund_id,
                                   t2.fund_id,
                                   t3.fund_id) as fund_id,
                          coalesce(t.price_date,
                                   t1.price_date,
                                   t2.price_date,
                                   t3.price_date) as price_date, ---月末日期
                          coalesce(t.adjusted_nav,
                                   t1.adjusted_nav,
                                   t2.adjusted_nav,
                                   t3.adjusted_nav) as adjusted_nav, --月末净值
                          t.ret_1m as ret_1m, --近一月收益
                          t.ret_1m_bmk as ret_1m_bmk, --近一月基准收益
                          t.ret_1m_a as ret_1m_a, --近一月年化收益
                          t.ret_1m_bmk_a as ret_1m_bmk_a, --近一月基准年化收益
                          t.excess_ret_1m as excess_ret_1m, --近一月超额收益
                          t.excess_ret_1m_a as excess_ret_1m_a, --近一月年化超额收益
                          t1.ret_1m as ret_3m, --近三月收益
                          t1.ret_1m_bmk as ret_3m_bmk, --近三月基准收益
                          t1.ret_1m_a as ret_3m_a, --近三月年化收益
                          t1.ret_1m_bmk_a as ret_3m_bmk_a, --近三月基准年化收益
                          t1.excess_ret_1m as excess_ret_3m, --近三月超额收益
                          t1.excess_ret_1m_a as excess_ret_3m_a, --近三月年化超额收益
                          t2.ret_1m as ret_6m, --近六月收益
                          t2.ret_1m_bmk as ret_6m_bmk, --近六月基准收益
                          t2.ret_1m_a as ret_6m_a, --近六月年化收益
                          t2.ret_1m_bmk_a as ret_6m_bmk_a, --近六月基准年化收益
                          t2.excess_ret_1m as excess_ret_6m, --近六月超额收益
                          t2.excess_ret_1m_a as excess_ret_6m_a, --近六月年化超额收益
                          t3.ret_1m as ret_1y, --近一年收益
                          t3.ret_1m_bmk as ret_1y_bmk, --近一年基准收益
                          t3.ret_1m_a as ret_1y_a, --近一年年化收益
                          t3.ret_1m_bmk_a as ret_1y_bmk_a, --近一年基准年化收益
                          t3.excess_ret_1m as excess_ret_1y, --近一年超额收益
                          t3.excess_ret_1m_a as excess_ret_1y_a --近一年年化超额收益
            from (select t.*
                    from dws1.dws_fund_performance_latest_tmp t
                   where t.ret_index_mark = 'ret_1m_a_index') t
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_3m_a_index') t1
              on t.fund_id = t1.fund_id
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_6m_a_index') t2
              on t.fund_id = t2.fund_id
            full join (select t.*
                        from dws1.dws_fund_performance_latest_tmp t
                       where t.ret_index_mark = 'ret_1y_a_index') t3
              on t.fund_id = t3.fund_id) t;
