-- [[ 주제 : 비트코인과 코로나19 현황 관계지어 살펴보기]] 
-- Q1) 코로나 누적 확진자수/사망자수/회복자수 및 전일대비 확진자수/사망자수/회복자수와 비트코인의 최대, 최소 등락폭 비교해보기 
-- Q2) 국가별 코로나 현황과 비트코인 최대, 최소 등락폭 비교해보기 
 

-- <테이블'bitcoin' - 컬럼 정보> 
-- Date : date of observation
-- Open : Opening price on the given day
-- High : Highest price on the given day
-- Low : Lowest price on the given day
-- Close : Closing price on the given day
-- Volume : Volume of transactions on the given day
-- Market Cap : Market capitalization in USD


-- <테이블 'covid_19' - 컬럼 정보>
-- Sno - Serial number
-- ObservationDate - Date of the observation in MM/DD/YYYY
-- Province/State - Province or state of the observation (Could be empty when missing)
-- Country/Region - Country of observation
-- Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
-- Confirmed - Cumulative number of confirmed cases till that date
-- Deaths - Cumulative number of of deaths till that date
-- Recovered - Cumulative number of recovered cases till that date 


-- <Strategy>
-- bitcoin 테이블 데이터 중 2020/1/22 ~ 2021/2/28 기간에 해당하는 데이터를 정체한 후 이를 covid_19 테이블과 조인 
-- bitcoin 테이블 데이터에는 다음의 컬럼을 추가 : price_gap(최고가-최저가), 전일대비 등락폭률 
-- 'covid_19' 테이블에는 임시테이블들을 통해 다음의 컬럼을 추가 : 일별 누적 확진자수/사망자수/회복자수, 전일대비 확진자/사망자/회복자 증감수 


-- [Setting] DB Proejct1 생성 후 bitcoin, covid_19 테이블 import 
create database Project1; 
use Project1; 
select * from bitcoin;
select * from covid_19;


-- [1]전체 테이블 컬럼 수정/ 추가  
-- [1-1]covid_19, bitcoin 날짜 형태 수정 
update covid_19 
set ObservationDate = CONCAT(SUBSTR(ObservationDate, 7, 4), '-', SUBSTR(ObservationDate, 1,2), '-', SUBSTR(ObservationDate, 4,2));

select * from bitcoin;
alter table bitcoin
add column Date_re text after Date; 
update bitcoin 
set Date_re = CONCAT(SUBSTR(Date, 1, 10));


-- [1-2] bitcoin 테이블에 다음 컬럼 추가: price_gap(최고가-최저가) 
ALTER TABLE bitcoin 
ADD COLUMN price_gap double after low;
UPDATE bitcoin
SET price_gap = high - low;


-- [3] 임시 테이블 만들기 : bitcoin2021, bitcoin_2021_final, covid_19_by_date, covid_19_by_date_country  
-- bitcoin_2021 : bitcoin 테이블에서 2020/01/22~2021/02/28에 해당되는 데이터 
-- bitcoin_2021_final : bitcoin_2021 테이블에 전일대비 등락폭율 ((당일price_gap -전일price_gap)/전일price_gap * 100) 컬럼 추가 
-- covid_19_by_date : covid_19 테이블에서 날짜별 누적 확진자, 사망자, 회복자 수 정제 
-- covid_19_by_date_country : covid_19 테이블에서 날짜별, 국가별 누적 확진자, 사망자, 회복자 수 정제 
-- covid_diff : 전일대비 확진자수, 사망자수, 회복자수 
-- bit_covid_by_date : 날짜별 비트코인, 코로나 현황(최종) 

with bitcoin_2021 AS( 
select * from bitcoin 
where Date_re >= '2020-01-22' AND Date_re <='2021-02-28'
ORDER BY Date_re
) 
, bitcoin_2021_final AS (
	select A.SNo as '시리얼 넘버' , A.Date_re as '날짜', 
		A.High as '최고가' ,A.Low as '최저가', A.price_gap as '등락폭', 
		(B.price_gap - A.price_gap)/A.price_gap as '전일대비 등락폭률' 
	from bitcoin_2021 A, bitcoin_2021 B 
	Where A.SNo=B.SNo-1
    ) 
, covid_by_date AS (
	select ObservationDate as '날짜',
    sum(Confirmed) as '누적 확진자수', sum(Deaths) as '누적 사망자수', sum(Recovered) as '누적 회복자수', 
    sum(Deaths)/sum(Confirmed) as '누적 사망율', sum(Recovered)/sum(Confirmed) as '누적 회복율' 
	from covid_19 
	group by ObservationDate
    )
, covid_by_date_country AS (
	select ObservationDate as '날짜', `Country/Region` as '국가',
    sum(Confirmed) as '누적 확진자수', sum(Deaths) as '누적 사망자수', sum(Recovered) as '누적 회복자수', 
    sum(Deaths)/sum(Confirmed) as '누적 사망율', sum(Recovered)/sum(Confirmed) as '누적 회복율'
	from covid_19 
	group by ObservationDate, `Country/Region`
    )   
, covid_diff As (
select tab.* from (select c.날짜 as 전날, c1.날짜 as 당일, c1.`누적 확진자수`- c.`누적 확진자수` as `전일대비 확진자증감`,
				c1.`누적 사망자수`- c.`누적 사망자수` as `전일대비 사망자증감`, c1.`누적 회복자수`- c.`누적 회복자수` as `전일대비 회복자증감`, 
				rank() over (partition by c.날짜 order by c1.날짜) as 날짜차 
					from covid_by_date c, covid_by_date c1
					where c.날짜 < c1.날짜 
					order by c.날짜, c1.날짜) tab
where tab.날짜차=1)
, bit_covid_by_date AS ( 
			select B.날짜, B.최고가, B.최저가, B.등락폭, B.`전일대비 등락폭률`, C1.`누적 확진자수`, C1.`누적 사망자수`, 
				C1.`누적 회복자수`, C2.`전일대비 확진자증감`, C2.`전일대비 사망자증감`, C2.`전일대비 회복자증감`, C1.`누적 사망율`, C1.`누적 회복율`
				from bitcoin_2021_final B, covid_by_date C1, covid_diff C2 
				where B.날짜 = C1.날짜 and B.날짜 = C2.전날)


-- [4] 데이터 조회하기 
-- [4-1] 날짜별 비트코인 및 코로나 현황 
-- select * from bit_covid_by_date;


-- [4-2] 전일대비 등락폭률이 가장 높았을 때, 낮았을 때  
-- 1) 높았을 때 날짜 : 2020/04/28
-- 누적 확진자 수 : 3,117,208 / 전일대비 확진자수 증감 : +78697 
-- 누적 사망자 수 : 218,517 / 누적 사망율 : 0.07 / 전일대비 사망자수 증감 : +10441
-- 누적 회복자 수 : 928,962 / 누적 회복율 : 0.30  / 전일대비 회복자수 증감 : +43956 

-- 2) 낮았을 때 날짜 : 2020/04/16 
-- 누적 확진자 수 : 2,152,147 /  전일대비 확진자수 증감 : +87647
-- 누적 사망자 수 : 144,607 / 누적 사망율 : 0.067 / 전일대비 사망자수 증감 : +10069
-- 누적 회복자 수 : 542,301 /  누적 회복율 : 0.25 / 전일대비 회복자수 증감 : +26297 

-- select *
-- from bit_covid_by_date
-- where `전일대비 등락폭률` = 
-- 	(select max(`전일대비 등락폭률`) 
--     from bit_covid_by_date)
-- UNION
-- select *
-- from bit_covid_by_date
-- where `전일대비 등락폭률` = 
-- 	(select min(`전일대비 등락폭률`) 
--     from bit_covid_by_date);




-- [4-3] 국가별, 날짜별 코로나 현황 + 비트코인 현황 
-- 국가별, 날짜별 전일대비 확진자/사망자/회복자수 증감 테이블을 정제하려 시도하였으나 데이터양이 너무 많아 오래 시간이 걸림. 
-- 따라서 이 부분은 중요한 국가들만 따로 추려 테이블을 정제한 후 시도하는게 더 효율있어보임. 

-- select c1.*, B.최고가, B.최저가, B.등락폭, B.`전일대비 등락폭률`  
-- from covid_by_date_country c1, bitcoin_2021_final B 
-- where c1.날짜 = B.날짜;



-- [4-4] 비트코인 등락폭이 최고일 때 국가별 코로나 현황
-- 누적 회복자수 기준 상위 10개국  

-- select C1.날짜, C1.국가, C1.`누적 확진자수`, C1.`누적 사망자수`, C1.`누적 회복자수`, C1.`누적 사망율`, C1.`누적 회복율`, 
-- 		B.최고가, B.최저가, B.등락폭, B.`전일대비 등락폭률`
-- from covid_by_date_country C1, bitcoin_2021_final B 
-- where B.`전일대비 등락폭률`=
-- 		(select max(`전일대비 등락폭률`)
-- 			from covid_by_date_country C1, bitcoin_2021_final B 
-- 			where B.날짜 = C1.날짜)
-- and B.날짜 = C1.날짜
-- order by C1.`누적 회복자수` DESC
-- limit 10;



 -- [4-5] 비트코인 등락폭이 최고일 때 국가별 코로나 현황
 -- 누적 회복자수 기준 상위 10개국  
-- select C1.날짜, C1.국가, C1.`누적 확진자수`, C1.`누적 사망자수`, C1.`누적 회복자수`, C1.`누적 사망율`, C1.`누적 회복율`, 
-- 		B.최고가, B.최저가, B.등락폭, B.`전일대비 등락폭률`
-- from covid_by_date_country C1, bitcoin_2021_final B 
-- where B.`전일대비 등락폭률`=
-- 		(select min(`전일대비 등락폭률`)
-- 			from covid_by_date_country C1, bitcoin_2021_final B 
-- 			where B.날짜 = C1.날짜)
-- and B.날짜 = C1.날짜
-- order by C1.`누적 회복자수` DESC
-- limit 10;






