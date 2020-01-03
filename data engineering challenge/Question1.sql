-----------------Create table
Drop table if exists dbo.pageview_activity_log;

create table dbo.pageview_activity_log (
ID int,
User_ID int,
Page_ID int,
Visit_Date date,
Visit_Time time
);

-------------Inserting values to table
insert into dbo.pageview_activity_log
values
(1,1,54,'2018-01-01','11:54:34'),
(2,1,55,'2018-01-01','11:55:10'),
(3,1,56,'2018-01-02','13:11:12'),
(4,1,55,'2018-01-02','17:10:08'),
(5,1,56,'2018-01-02','17:12:45'),
(6,2,55,'2018-01-01','10:25:18'),
(7,2,55,'2018-01-01','17:30:12'),
(8,2,55,'2018-01-01','17:45:57'),
(9,3,54,'2018-01-02','00:00:12'),
(1,3,56,'2018-01-02','00:03:22'),
(1,3,55,'2018-01-02','01:20:11'),
(1,3,56,'2018-01-02','01:40:09')
;

-----------Query to to find the total number of user sessions each page has each day. 

select 
	page_id,visit_date, 
	sum(case when session_diff is null then 1
			 when session_diff > 10 then 1
			else 0 end )total_user_sessions from
(
	select *,DATEDIFF(minute,visit_time,next_visit_time) as session_diff from
		(
		select 
			*,
			LEAD(visit_time) over(partition by page_id,visit_date,user_id order by visit_time) as next_visit_time
		from pageview_activity_log 
		)a
)b
group by page_id,visit_date;

----Output

page_id	visit_date	Total_User_Sessions
54		2018-01-01		1
54		2018-01-02		1
55		2018-01-01		4
55		2018-01-02		2
56		2018-01-02		4