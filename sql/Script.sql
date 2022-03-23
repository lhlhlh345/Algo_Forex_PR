

--- check symbols
select * from forex_precious_metal_symbol_jason_stage;



--- check daily candle
select * from forex_precious_metal_daily_jason_stage;

select count(1) from forex_precious_metal_daily_jason_stage;

select count(distinct forex_symbol) from forex_precious_metal_daily_jason_stage;

select forex_symbol, count(1) as cnt from forex_precious_metal_daily_jason_stage group by forex_symbol;

--- delete from forex_precious_metal_daily_jason_stage where time_stamp_nyc = '2022-03-16 17:00:00.000'; 

-- check usd_cad
select * from forex_precious_metal_daily_jason_stage where forex_symbol = 'OANDA:USD_CAD' order by time_stamp_nyc desc;



--- create index
create unique index idx_daily_forex_previous_metal on forex_precious_metal_daily_jason_stage (time_stamp_nyc, forex_symbol);


--- create index
create unique index idx_1m on forex_precious_metal_1m_jason_stage (time_stamp_nyc, forex_symbol);


--- check 1m candle
select count(1) from forex_precious_metal_1m_jason_stage where forex_symbol = 'OANDA:USD_CAD';

select * from forex_precious_metal_1m_jason_stage where forex_symbol = 'OANDA:USD_CAD' order by time_stamp_nyc desc;

select max(time_stamp_nyc), min(time_stamp_nyc) from forex_precious_metal_1m_jason_stage;

select max(time_stamp_nyc), min(time_stamp_nyc) from forex_precious_metal_1m_jason_stage where forex_symbol = 'OANDA:USD_CAD';


select * from forex_precious_metal_1m_jason_stage where date(time_stamp_nyc) between '2021-12-29' and '2022-01-03' and forex_symbol = 'OANDA:AUD_NZD'; 


select * from forex_precious_metal_1m_jason_stage where date(time_stamp_nyc) between '2021-12-29' and '2022-01-03' and forex_symbol = 'OANDA:USD_CAD'; 



---- delete 
delete from forex_precious_metal_1m_jason_stage where date(time_stamp_nyc) between '2022-02-01' and '2022-03-20';

delete from forex_precious_metal_1m_jason_stage where date(time_stamp_nyc) = '2022-03-21';





