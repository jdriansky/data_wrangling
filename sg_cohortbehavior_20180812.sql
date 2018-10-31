/************************************************
  --the name of this query is sg_cohortbehavior_20180812.sql
   goal--Key insights about LTV curves
   goal--Recommendations on business opportunities and follow up analysis
   goal--ESPN in Q4, Only reach non-SG customers, Want a 2X return over a 5 year time frame.  What's allowable CAC?
*************************************************/

   --MAKE A TABLE THAT SHOWS APPROPRIATE USER AMOUNTS FOR EACH MONTH

use DataDictionary

IF (OBJECT_ID('tempdb.dbo.#sg_numuser_trans1') IS NOT NULL)
		Drop Table #sg_numuser_trans1

SELECT		nu.concat_cohort,
			nu.cohort_month,
			nu.channel_group,
			nu.platform,
			nu.campaign_type,
			cast(nu.num_users as int) as num_users,
			cast(gt.days_aged as int) as days_aged,
			cast(gt.gross_transaction_value as int) as gross_transaction_value,
			cast(gt.gross_transaction_value as float) / cast(nu.num_users as float) as gtv_per_user
INTO	#sg_numuser_trans1
FROM	sg_num_users nu
		inner join sg_gross_transaction gt
			on	nu.cohort_month = gt.cohort_month
			and nu.channel_group = gt.channel_group
			and	nu.platform = gt.platform
			and nu.campaign_type = gt.campaign_type
		inner join (	SELECT		cohort_month,
									channel_group,
									platform,
									campaign_type,
									max(cast(days_aged as int)) as max_days_aged
						from sg_gross_transaction
						where gross_transaction_value > 0 --maximum age of cohort, could have 0 to start
						group by	cohort_month,
									channel_group,
									platform,
									campaign_type
					) max_age
			on	gt.cohort_month = max_age.cohort_month
			and gt.channel_group = max_age.channel_group
			and	gt.platform = max_age.platform
			and gt.campaign_type = max_age.campaign_type
			and cast(gt.days_aged as int) <= cast(max_days_aged as int)

  --get standard deviations and distance from standard deviation for each data point of a given age
  --group by everything except cohort_month

  --get weighted average gtv by relevant components

IF (OBJECT_ID('tempdb.dbo.#sg_wtdavg_stdev1') IS NOT NULL)
		Drop Table #sg_wtdavg_stdev1

select		channel_group,
			sum(num_users * gtv_per_user) / sum(num_users) as wtd_average_gtv,
			sum(num_users) as tot_users,
			count(*) as num_obs
into	#sg_wtdavg_stdev1
from	#sg_numuser_trans1
where		days_aged = 0
group by	channel_group


  --get wtd average standard deviation

IF (OBJECT_ID('tempdb.dbo.#sg_wtdavg_stdev2') IS NOT NULL)
		Drop Table #sg_wtdavg_stdev2

select		a.channel_group,
			a.days_aged,
			max(b.num_obs) as num_obs,
			max(b.wtd_average_gtv) as wtd_average_gtv,
			SQRT(sum((cast(a.num_users as float)/cast(b.tot_users as float)) 
				* power(cast(a.gtv_per_user as float) - cast(b.wtd_average_gtv as float),2))) as wtd_stdev_gtv
into	#sg_wtdavg_stdev2
from	#sg_numuser_trans1 a
		inner join #sg_wtdavg_stdev1 b
			on	a.channel_group = b.channel_group
group by	a.channel_group,
			a.days_aged

  --join weighted average standard deviation back to original table
  --also create a field showing that a campaign was representative of the ESPN campaign

IF (OBJECT_ID('tempdb.dbo.#sg_numuser_trans2') IS NOT NULL)
		Drop Table #sg_numuser_trans2

select		snt1.*,
			SUBSTRING(snt1.cohort_month,1,4) as cohort_year,
			SUBSTRING(snt1.cohort_month,5,2) as cohort_month_number,
			case	when snt1.channel_group in ('Audio','SEM Branded','SEO Branded','Influencer')
						and	SUBSTRING(snt1.cohort_month,5,2) in ('10','11','12')
						then 1 else 0 
			end 	as flag_espn_cohort,
			sda.wtd_average_gtv,
			sda.wtd_stdev_gtv,
			case when abs(snt1.gtv_per_user - wtd_average_gtv) > 1 * sda.wtd_stdev_gtv 
				then 1 else 0 end as flag_outside_range  --cuts out 4% of total users
into	#sg_numuser_trans2
from	#sg_numuser_trans1 snt1
		inner join #sg_wtdavg_stdev2 sda
			on	snt1.channel_group = sda.channel_group
			and	snt1.days_aged = sda.days_aged

  --Bundle a group of cohorts together to resemble ESPN ads
  --Sort these bundles by level of success at 0 days old

IF (OBJECT_ID('tempdb.dbo.#espn_group1') IS NOT NULL)
		Drop Table #espn_group1

select		snt2.*,
			percentile_gtv_per_user,
			case	when percentile_gtv_per_user <= 0.2 then 'Bottom 20%'
					when percentile_gtv_per_user >= 0.8 then 'Top 20%'
					else 'Middle 60%'
			end as campaign_performance_group
into	#espn_group1
FROM	#sg_numuser_trans2  snt2
		inner join (	select		concat_cohort,
									PERCENT_RANK() OVER (order by gtv_per_user) as percentile_gtv_per_user
						from	#sg_numuser_trans2
						WHERE		days_aged = 360
								and flag_outside_range = 0
								and	flag_espn_cohort = 1
					) rnk
				on snt2.concat_cohort = rnk.concat_cohort
WHERE		snt2.flag_outside_range = 0
		and	snt2.flag_espn_cohort = 1

  --output espn growth curves

select		campaign_performance_group,
			days_aged,
			sum(gross_transaction_value) as gross_transaction_value,
			sum(num_users) as num_users
from	#espn_group1
group by	campaign_performance_group,
			days_aged;

  --check scalability of espn campaigns

select		concat_cohort,
			channel_group,
			campaign_performance_group,
			gross_transaction_value,
			num_users
from #espn_group1
where days_aged = 360
order by concat_cohort;

/************************************************
  --Outputs begin here
*************************************************/

  --output the data by channel

select		a.channel_group,
			a.days_aged,
			sum(a.gross_transaction_value) as gross_transaction_value,
			sum(a.num_users) as num_users
from	#sg_numuser_trans2 a
		inner join (	select		channel_group,	
									days_aged,
									sum(num_users) as tot_users,
									count(*) as tot_cohorts
						from	#sg_numuser_trans2
						group by	channel_group,	
									days_aged
					) b
			on a.channel_group = b.channel_group
			and a.days_aged = b.days_aged
where	flag_outside_range = 0
	and b.tot_users > 2000
	and tot_cohorts >= 3
group by	a.channel_group,
			a.days_aged;

  --output across platforms

select		a.platform,
			a.days_aged,
			sum(a.gross_transaction_value) as gross_transaction_value,
			sum(a.num_users) as num_users
from	#sg_numuser_trans2 a
		inner join (	select		platform,	
									days_aged,
									sum(num_users) as tot_users,
									count(*) as tot_cohorts
						from	#sg_numuser_trans2
						where	flag_outside_range = 0
						group by	platform,	
									days_aged
					) b
			on a.platform = b.platform
			and a.days_aged = b.days_aged
where	a.flag_outside_range = 0
	and	b.tot_users > 2000
group by	a.platform,
			a.days_aged;

  --output across years

select		a.cohort_year,
			a.days_aged,
			sum(a.gross_transaction_value) as gross_transaction_value,
			sum(a.num_users) as num_users
from	#sg_numuser_trans2 a
		inner join (	select		cohort_year,	
									days_aged,
									sum(num_users) as tot_users,
									count(*) as tot_cohorts
						from	#sg_numuser_trans2
						where	flag_outside_range = 0
						group by	cohort_year,	
									days_aged
					) b
			on a.cohort_year = b.cohort_year
			and a.days_aged = b.days_aged
where	a.flag_outside_range = 0
	and	b.tot_users > 2000
group by	a.cohort_year,
			a.days_aged;

  --output across month numbers

select		a.cohort_month_number,
			a.days_aged,
			sum(a.gross_transaction_value) as gross_transaction_value,
			sum(a.num_users) as num_users
from	#sg_numuser_trans2 a
		inner join (	select		cohort_month_number,	
									days_aged,
									sum(num_users) as tot_users,
									count(*) as tot_cohorts
						from	#sg_numuser_trans2
						where	flag_outside_range = 0
						group by	cohort_month_number,	
									days_aged
					) b
			on a.cohort_month_number = b.cohort_month_number
			and a.days_aged = b.days_aged
where	a.flag_outside_range = 0
	and	b.tot_users > 2000
group by	a.cohort_month_number,
			a.days_aged;

  --output across campaigns

select		a.campaign_type,
			a.days_aged,
			sum(a.gross_transaction_value) as gross_transaction_value,
			sum(a.num_users) as num_users
from	#sg_numuser_trans2 a
		inner join (	select		cohort_month_number,	
									days_aged,
									sum(num_users) as tot_users,
									count(*) as tot_cohorts
						from	#sg_numuser_trans2
						where	flag_outside_range = 0
						group by	cohort_month_number,	
									days_aged
					) b
			on a.cohort_month_number = b.cohort_month_number
			and a.days_aged = b.days_aged
where	a.flag_outside_range = 0
	and	b.tot_users > 2000
group by	a.campaign_type,
			a.days_aged;

  --check scalability of the app

select		concat_cohort,
			gross_transaction_value,
			num_users
from #sg_numuser_trans2
where platform = 'iOS App'
	and days_aged = 360
	and flag_outside_range = 0

  --check scalability of web

select		concat_cohort,
			channel_group,
			gross_transaction_value,
			num_users
from #sg_numuser_trans2
where platform = 'web'
	and days_aged = 360
	and flag_outside_range = 0


