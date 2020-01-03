
use edureka_dw;
drop table if exists edureka_dw.seat_booking_theatre_3;
create table edureka_dw.seat_booking_theatre_3 as
select 
            seat_no,
            coalesce(movie_date,'2019-11-03') as movie_date,
            theatre.id,
            theatre_name,
            lead(seat_no,1,0) over(partition by theatre.id,theatre.row,movie_date order by seat_no asc) as next_seat , 
            theatre.total_seats_in_row,
            theatre.`row` 
        from (select * from seat_bookings where movie_date ='2019-11-03') seat_bookings
        right join 
        (
            select t.* from 
            theatre t inner join 
            (
                select distinct theatre_id from seat_bookings where movie_date ='2019-11-03' 
            )s 
            on t.id = s.theatre_id 
        )theatre
         on seat_bookings.theatre_id= theatre.id and seat_bookings.`row`= theatre.`row`
;


use edureka_dw;
drop table if exists edureka_dw.seat_booking_theatre_4;
create table edureka_dw.seat_booking_theatre_4 as
select 
            seat_no,
            coalesce(movie_date,'2019-11-04') as movie_date,
            theatre.id,
            theatre_name,
            lead(seat_no,1,0) over(partition by theatre.id,theatre.row,movie_date order by seat_no asc) as next_seat , 
            theatre.total_seats_in_row,
            theatre.`row` 
        from (select * from seat_bookings where movie_date ='2019-11-04') seat_bookings
        right join 
        (
            select t.* from 
            theatre t inner join 
            (
                select distinct theatre_id from seat_bookings where movie_date ='2019-11-04' 
            )s 
            on t.id = s.theatre_id 
        )theatre
         on seat_bookings.theatre_id= theatre.id and seat_bookings.`row`= theatre.`row`
;


use edureka_dw;
drop table if exists edureka_dw.seat_booking_theatre_5;
create table edureka_dw.seat_booking_theatre_5 as
select 
            seat_no,
            coalesce(movie_date,'2019-11-05') as movie_date,
            theatre.id,
            theatre_name,
            lead(seat_no,1,0) over(partition by theatre.id,theatre.row,movie_date order by seat_no asc) as next_seat , 
            theatre.total_seats_in_row,
            theatre.`row` 
        from (select * from seat_bookings where movie_date ='2019-11-05') seat_bookings
        right join 
        (
            select t.* from 
            theatre t inner join 
            (
                select distinct theatre_id from seat_bookings where movie_date ='2019-11-05' 
            )s 
            on t.id = s.theatre_id 
        )theatre
         on seat_bookings.theatre_id= theatre.id and seat_bookings.`row`= theatre.`row`
;

  


drop table if exists empty_seats;     
create table empty_seats as
select distinct *
from (
select
        movie_date,
        id as theatre_id,
        theatre_name,
        row,
        seat_no,
        next_seat,
        total_seats_in_row,
        case 
-- if record is not last record in partition then  find number of empty seats between two given seats
            when next_seat <> 0 then (next_seat-seat_no)-1 
            -- if record is last record in partition and seat is not last seat in the row 
when next_seat=0 and total_seats_in_row <> seat_no then (total_seats_in_row-seat_no) 
-- if record is last record in partition and seat is last seat in the row 
            when next_seat=0 and total_seats_in_row = seat_no then (total_seats_in_row-seat_no)
-- if no booking done in the row
            when seat_no is null  then total_seats_in_row
            end as empty_seats,
        m.movie_id,
        m.movie_name

    from
        (
select * from seat_booking_theatre_3
union all
select * from seat_booking_theatre_4
union all
select * from seat_booking_theatre_5
        )stg  
        inner join movies m
        on m.theatre_id = stg.id
) a where empty_seats >=3
order by movie_date,theatre_name,row,seat_no
;


