# mustard 
-
=======================================================================
Python library Imports
=======================================================================

-- sudo pip install flatten_json
-- sudo pip install json

-
=======================================================================
--Python script that will parse these files, 
--and read them into a database with that schema
=======================================================================

-- dataLoad.py
-- db_config.py
-- db_info.ini  (This contains connection details to posgreSQL database)

=======================================================================
-- DATA MODELLING
-- Mustard balls tables creation 
-- SQL database schema for representing these balls.
======================================================================

create schema cricket;

--Drop tables if they already exists
drop table cricket.matches;
drop table cricket.teams;
drop table cricket.players;

CREATE TABLE cricket.matches (
    row_id INT GENERATED ALWAYS AS IDENTITY,
    match_id INT NOT null,
    is_out VARCHAR(100) not null,
    runs int not null,
    batting_team_id INT NOT null ,
    bowling_team_id INT NOT null ,
    batter_id int not null,
    non_facer_id INT NOT null ,
    bowler_id INT NOT null ,
    created_date timestamp default now(),
    PRIMARY KEY(match_id,batting_team_id,bowling_team_id,batter_id,non_facer_id,bowler_id)
);

CREATE TABLE cricket.teams (
    row_id INT GENERATED ALWAYS AS IDENTITY,
    team_id INT NOT null unique,
    name VARCHAR(100) not null,
    created_date timestamp default now(),
    PRIMARY KEY(row_id)
);


CREATE TABLE cricket.players (
    row_id INT GENERATED ALWAYS AS IDENTITY,
    player_id INT NOT null unique,
    hand VARCHAR(100) not null,
    name VARCHAR(100) not null,
    created_date timestamp default now(),
    PRIMARY KEY(row_id)
);


select * from cricket.matches;
select * from cricket.teams;
select * from cricket.players;



=============================================================================
QUERIES
=============================================================================

-- Question 1 
-- Which team won each match? (i.e. who had the cumulative 
-- highest number of runs in their innings)
-----------------------------------------------------------------

with winning_team as 
(
	select 
		a.* ,rank() over (partition by a.match_id order by a.match_id,a.total desc)  rnk
	from (select  match_id , batting_team_id,bowling_team_id , sum(runs) as total 
			from cricket.matches cm 
			group by match_id, batting_team_id,bowling_team_id 
		)  as a 
) 
select wt.match_id , t1.name as "Batting team" ,t2.name as "Bowling Team" , wt.total ,
	case when rnk = 1 then t1.name else '' end as WINNER  
from winning_team wt
	left join teams t1
		on wt.batting_team_id = t1.team_id 
	left join teams t2 
		on wt.bowling_team_id = t2.team_id 
--where rnk = 1
order by 1, 2 ;



-- Question 2
-- Which “over” had the highest score for each team for each match? 
-- (i.e. for balls in this over number, which had the cumulative highest total)
--------------------------------------------------------------------------------

with over_total as 
(
	select  match_id, bowling_team_id, count(1) ball_total, count(1)/6 over_cnt
		,rank() over (partition by match_id order by count(1) desc)  rnk 
	from cricket.matches m  
		left join teams t 
			on bowling_team_id = t.team_id
	group by match_id,bowling_team_id
	order by 1 
)
select  
	ot.match_id , t.name as team, ball_total, over_cnt, 
	case when rnk = 1 then t.name else '' end as "Highest Total"
from over_total ot
	left join teams t 
		on ot.bowling_team_id = t.team_id
--where rnk = 1
order by ot.match_id, ball_total ;


-- Question 3
-- The average number of runs scored by each batter 
-- across all the matches they played in.
---------------------------------------------------------

select   
	batter_id , p.name , count(1) match_played_in, sum(runs) as total_runs ,
	sum(runs)/count(1) ave_no_of_runs
from cricket.matches cm 
	inner join players p
		on cm.batter_id = p.player_id
group by  batter_id , p.name order by 1
	

		