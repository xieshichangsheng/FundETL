--�ѽ�������ĩ���ӽ���ĩ���Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                 row_number() over(partition by t.fund_id order by t1.calendar_id desc) as day_mins --ȡ���¾�ֵ����
            from (select t.price_date as calendar_id, 1 as is_trd_day
                    from dwd1.dwd_index_nav_di t
                   where t.index_id = 'IN00000001') t1
            full join dwd1.dwd_fund_nav_di t
              on t.nav_date = t1.calendar_id
           where t.dt = (select max(dt) from dwd1.dwd_fund_nav_di)
           order by t1.calendar_id desc) t
   where t.day_mins = 1;

--�ѽ�������һ�ܣ��ӽ������壩�Ļ���Ȩ��ֵ���뵽��ʱ�б�
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

--�ѽ�������һ���³����ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                     and t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                          and t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ������������³����ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                     and t1.calendar_id <= t2.last_month --С������ǰȡ���һ�쾻ֵ
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
                                          and t1.calendar_id >= t2.last_month --��������ǰȡ���һ�쾻ֵ
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ������������³����ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                     and t1.calendar_id <= t2.last_month --С������ǰȡ���һ�쾻ֵ
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
                                          and t1.calendar_id >= t2.last_month --��������ǰȡ���һ�쾻ֵ
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ�������һ������ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                     and t1.calendar_id <= t2.last_month --С��1��ǰȡ���һ�쾻ֵ
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
                                          and t1.calendar_id >= t2.last_month --����1��ǰȡ���һ�쾻ֵ
                                        order by t1.calendar_id desc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ��������������³����ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                                             -1)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -1)) --�����³�ȡ���һ�쾻ֵ
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ��������������³����ӽ��³����Ļ���Ȩ��ֵ���뵽��ʱ�б�
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
                                                             -11)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -11)) --�����³�ȡ���һ�쾻ֵ
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ�������ĩ���ӽ���ĩ����ָ����ֵ���뵽��ʱ�б�
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
                 row_number() over(partition by t.index_id order by t1.calendar_id desc) as day_mins --ȡ���¾�ֵ����
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
                 row_number() over(partition by t.index_id order by t1.calendar_id desc) as day_mins --ȡ���¾�ֵ����
            from (select t.price_date as calendar_id, 1 as is_trd_day
                    from dwd1.dwd_index_nav_di t
                   where t.index_id = 'IN00000015') t1
            full join dwd1.dwd_index_nav_di t
              on t.price_date = t1.calendar_id
           where t.index_id = 'IN00000015'
           order by t1.calendar_id desc) t
   where t.day_mins = 1;

--�ѽ�������һ�ܣ��ӽ������壩��ָ����ֵ���뵽��ʱ�б�
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
            date_sub(date_sub(next_day(CURRENT_DATE, 'FR'), 7), 5)) t --������ǰ5��ȡ���һ��ָ�����̼�
   where rn = 1;

--�ѽ�������һ���³����ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                   where t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
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
                                   where t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ������������³����ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                   where t1.calendar_id <= t2.last_month --С������ǰȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ǰȡ���һ�쾻ֵ
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
                                   where t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ������������³����ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                   where t1.calendar_id <= t2.last_month --С������ǰȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ǰȡ���һ�쾻ֵ
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
                                   where t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ�������һ������ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                   where t1.calendar_id <= t2.last_month --С������ǰȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ǰȡ���һ�쾻ֵ
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
                                   where t1.calendar_id <= t2.last_month --С������ȡ���һ�쾻ֵ
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
                                        where t1.calendar_id >= t2.last_month --��������ȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t
                                where t.day_mins <= 5) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ��������������³����ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                                             -1)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -1)) --������ĩȡ���һ�쾻ֵ
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
                                                             -1)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -1)) --������ĩȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--�ѽ��������������³����ӽ��³�����ָ����ֵ���뵽��ʱ�б�
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
                                                             -11)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -11)) --������ĩȡ���һ�쾻ֵ
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
                                                             -11)) --С���³�ȡ���һ�쾻ֵ
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
                                                                  -11)) --������ĩȡ���һ�쾻ֵ
                                          and t.index_id = 'IN00000015'
                                        order by t1.calendar_id asc) t) t) t
                where t.rn = 1
                order by t.calendar_id desc) t1
      on t.fund_id = t1.fund_id;

--��������һ�����棬��׼����
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --������ĩ��ֵ
                  t.nav_date,
                  t.adjusted_nav,
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --��һ�ܻ�׼����
                  null as ret_m_a,
                  null as ret_m_bmk_a,
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --��һ�ܳ�������
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
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--��������һ�������棬�껯����
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --��һ�»�׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --��һ���껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --��һ�»�׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --��һ�³�������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --��һ���껯��������
                  'ret_1m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_1month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--�����������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --����������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --�����»�׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --�������껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --�����»�׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --�����³�������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --�������껯��������
                  'ret_3m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_3month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--�����������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --����������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --�����»�׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --�������껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --�����»�׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --�����³�������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --�������껯��������
                  'ret_6m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_6month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--��������һ�����棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --����������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --�����»�׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --�������껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --�����»�׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --�����³�������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --�������껯��������
                  'ret_1y_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_1year_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����������������棬�껯����
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��������������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --����������׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --���������껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --����������׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --����������������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --���������껯��������
                  'ret_m_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_month_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--�����������������棬�껯����
insert overwrite table dws1.dws_fund_performance_latest_tmp
  select distinct nvl(t.fund_id, t1.fund_id) as fund_id,
                  substr(from_unixtime(unix_timestamp(t1.nav_date),
                                       'yyyy-MM-dd'),
                         1,
                         7) as end_date,
                  t1.nav_date as price_date, ---��ĩ����
                  t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��������������
                  (t1.index_nav - innav.close) / innav.close as ret_m_bmk, --����������׼����
                  POWER((1 + (t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --���������껯����
                  POWER((1 + (t1.index_nav - innav.close) / innav.close),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_bmk_a, --����������׼�껯����
                  (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav -
                  (t1.index_nav - innav.close) / innav.close as excess_ret_m, --����������������
                  POWER((1 + ((t1.adjusted_nav - t.adjusted_nav) /
                        t.adjusted_nav -
                        (t1.index_nav - innav.close) / innav.close)),
                        365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as excess_ret_m_a, --���������껯��������
                  'ret_y_a_fund' as ret_index_mark
    from (select t.*
            from dws1.dws_fund_performance_latest_edadj_tmp t
           where t.adjusted_nav_mark = 'early_year_endday_fund') t
    left join (select t.fund_id, t.strategy_category_id_primary
                 from dwd1.dwd_fund_info_di t
                where dt = (select max(dt) from dwd1.dwd_fund_info_di)
                  and t.strategy_category_id_primary is not null) fi --dwd_fund_info_di
      on t.fund_id = fi.fund_id
    left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
      on fi.strategy_category_id_primary = strategy.strategy_category_id
    left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
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
                 left join dwh1.ods_irp_strategy_category_df strategy ---��ͨ��dwd_fund_info_di.strategy_category_id_primary�ҵ���׼ָ��id
                   on fi.strategy_category_id_primary =
                      strategy.strategy_category_id
                 left join dwd1.dwd_index_nav_di innav --������ָ��nav��ȡָ�����׼����
                   on strategy.benchmark_id = innav.index_id
                  and t.nav_date = innav.price_date) t1
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ����һ������
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         t.nav_date,
         t1.adjusted_nav,
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
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
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ����һ�������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --��һ���껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_1m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_1month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ�������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --��һ���껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_3m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_3month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ�������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --��һ���껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_6m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_6month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ����һ�����棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --��һ������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --��һ���껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_1y_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_1year_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ�������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --������������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --���������껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_m_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_month_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--����ָ�������������棬�껯����
insert into table dws1.dws_fund_performance_latest_tmp
  select nvl(t.fund_id, t1.fund_id) as fund_id,
         substr(from_unixtime(unix_timestamp(t1.nav_date), 'yyyy-MM-dd'),
                1,
                7) as end_date, ---��ĩ
         t1.nav_date as price_date, ---��ĩ����
         t1.adjusted_nav as adjusted_nav, --��ĩ��ֵ
         t.nav_date,
         t1.adjusted_nav,
         (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav as ret_m, --������������
         null as ret_m_bmk,
         POWER((1 + (t1.adjusted_nav - t.adjusted_nav) / t.adjusted_nav),
               365.25 / datediff(t1.nav_date, t.nav_date)) - 1 as ret_m_a, --���������껯����
         null as ret_m_bmk_a,
         null as excess_ret_m,
         null as excess_ret_m_a,
         'ret_y_a_index' as ret_index_mark
    from (select t1.*
            from dws1.dws_fund_performance_latest_edadj_tmp t1
           where t1.adjusted_nav_mark = 'early_year_endday_index') t
    full join (select t1.*
                 from dws1.dws_fund_performance_latest_edadj_tmp t1
                where t1.adjusted_nav_mark = 'end_month_endday_index') t1 --����ָ����ĩ��ֵ
      on t.fund_id = t1.fund_id
   where t1.nav_date > t.nav_date;

--������ָ���������Ŀ���
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
                  t.ret_1m --��һ������
                 ,
                  t.ret_1m_bmk --��һ�»�׼����
                 ,
                  t.ret_1m_a --��һ���껯����
                 ,
                  t.ret_1m_bmk_a --��һ�»�׼�껯����
                 ,
                  t.excess_ret_1m --��һ�³�������
                 ,
                  t.excess_ret_1m_a --��һ���껯��������
                 ,
                  t.ret_3m --����������
                 ,
                  t.ret_3m_bmk --�����»�׼����
                 ,
                  t.ret_3m_a --�������껯����
                 ,
                  t.ret_3m_bmk_a --�����»�׼�껯����
                 ,
                  t.excess_ret_3m --�����³�������
                 ,
                  t.excess_ret_3m_a --�������껯��������
                 ,
                  t.ret_6m --����������
                 ,
                  t.ret_6m_bmk --�����»�׼����
                 ,
                  t.ret_6m_a --�������껯����
                 ,
                  t.ret_6m_bmk_a --�����»�׼�껯����
                 ,
                  t.excess_ret_6m --�����³�������
                 ,
                  t.excess_ret_6m_a --�������껯��������
                 ,
                  t.ret_1y --��һ������
                 ,
                  t.ret_1y_bmk --��һ���׼����
                 ,
                  t.ret_1y_a --��һ���껯����
                 ,
                  t.ret_1y_bmk_a --��һ���׼�껯����
                 ,
                  t.excess_ret_1y --��һ�곬������
                 ,
                  t.excess_ret_1y_a --��һ���껯��������
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
                                   t3.price_date) as price_date, ---��ĩ����
                          coalesce(t.adjusted_nav,
                                   t1.adjusted_nav,
                                   t2.adjusted_nav,
                                   t3.adjusted_nav) as adjusted_nav, --��ĩ��ֵ
                          t.ret_1m as ret_1m, --��һ������
                          t.ret_1m_bmk as ret_1m_bmk, --��һ�»�׼����
                          t.ret_1m_a as ret_1m_a, --��һ���껯����
                          t.ret_1m_bmk_a as ret_1m_bmk_a, --��һ�»�׼�껯����
                          t.excess_ret_1m as excess_ret_1m, --��һ�³�������
                          t.excess_ret_1m_a as excess_ret_1m_a, --��һ���껯��������
                          t1.ret_1m as ret_3m, --����������
                          t1.ret_1m_bmk as ret_3m_bmk, --�����»�׼����
                          t1.ret_1m_a as ret_3m_a, --�������껯����
                          t1.ret_1m_bmk_a as ret_3m_bmk_a, --�����»�׼�껯����
                          t1.excess_ret_1m as excess_ret_3m, --�����³�������
                          t1.excess_ret_1m_a as excess_ret_3m_a, --�������껯��������
                          t2.ret_1m as ret_6m, --����������
                          t2.ret_1m_bmk as ret_6m_bmk, --�����»�׼����
                          t2.ret_1m_a as ret_6m_a, --�������껯����
                          t2.ret_1m_bmk_a as ret_6m_bmk_a, --�����»�׼�껯����
                          t2.excess_ret_1m as excess_ret_6m, --�����³�������
                          t2.excess_ret_1m_a as excess_ret_6m_a, --�������껯��������
                          t3.ret_1m as ret_1y, --��һ������
                          t3.ret_1m_bmk as ret_1y_bmk, --��һ���׼����
                          t3.ret_1m_a as ret_1y_a, --��һ���껯����
                          t3.ret_1m_bmk_a as ret_1y_bmk_a, --��һ���׼�껯����
                          t3.excess_ret_1m as excess_ret_1y, --��һ�곬������
                          t3.excess_ret_1m_a as excess_ret_1y_a --��һ���껯��������
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
                                   t3.price_date) as price_date, ---��ĩ����
                          coalesce(t.adjusted_nav,
                                   t1.adjusted_nav,
                                   t2.adjusted_nav,
                                   t3.adjusted_nav) as adjusted_nav, --��ĩ��ֵ
                          t.ret_1m as ret_1m, --��һ������
                          t.ret_1m_bmk as ret_1m_bmk, --��һ�»�׼����
                          t.ret_1m_a as ret_1m_a, --��һ���껯����
                          t.ret_1m_bmk_a as ret_1m_bmk_a, --��һ�»�׼�껯����
                          t.excess_ret_1m as excess_ret_1m, --��һ�³�������
                          t.excess_ret_1m_a as excess_ret_1m_a, --��һ���껯��������
                          t1.ret_1m as ret_3m, --����������
                          t1.ret_1m_bmk as ret_3m_bmk, --�����»�׼����
                          t1.ret_1m_a as ret_3m_a, --�������껯����
                          t1.ret_1m_bmk_a as ret_3m_bmk_a, --�����»�׼�껯����
                          t1.excess_ret_1m as excess_ret_3m, --�����³�������
                          t1.excess_ret_1m_a as excess_ret_3m_a, --�������껯��������
                          t2.ret_1m as ret_6m, --����������
                          t2.ret_1m_bmk as ret_6m_bmk, --�����»�׼����
                          t2.ret_1m_a as ret_6m_a, --�������껯����
                          t2.ret_1m_bmk_a as ret_6m_bmk_a, --�����»�׼�껯����
                          t2.excess_ret_1m as excess_ret_6m, --�����³�������
                          t2.excess_ret_1m_a as excess_ret_6m_a, --�������껯��������
                          t3.ret_1m as ret_1y, --��һ������
                          t3.ret_1m_bmk as ret_1y_bmk, --��һ���׼����
                          t3.ret_1m_a as ret_1y_a, --��һ���껯����
                          t3.ret_1m_bmk_a as ret_1y_bmk_a, --��һ���׼�껯����
                          t3.excess_ret_1m as excess_ret_1y, --��һ�곬������
                          t3.excess_ret_1m_a as excess_ret_1y_a --��һ���껯��������
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
