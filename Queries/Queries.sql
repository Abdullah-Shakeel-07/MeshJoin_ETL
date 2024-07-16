-- Q1

-- select p.PRODUCT_ID, s.SUPPLIER_ID, d.QTR as Quarterly, d.MM as Monthly, sum(sa.TOTAL_SALE) as Total_sale
-- from warehouse.sales sa, warehouse.suppliers s, warehouse.products p, warehouse.dates d
-- where p.PRODUCT_ID = sa.PRODUCT_ID and sa.SUPPLIER_ID = s.SUPPLIER_ID and sa.DATE_ID = d.DATE_ID
-- group by PRODUCT_ID, SUPPLIER_ID, Monthly, Quarterly 
-- order by PRODUCT_ID;


-- Q2

-- select s.STORE_ID, p.PRODUCT_ID, SUM(sa.TOTAL_SALE) as Total_sale
-- from warehouse.products p, warehouse.sales sa, warehouse.stores s
-- where sa.STORE_ID = s.STORE_ID and sa.PRODUCT_ID = p.PRODUCT_ID
-- group by STORE_ID, PRODUCT_ID
-- order by STORE_ID, PRODUCT_ID;

-- Q3

-- select p.PRODUCT_ID, d.WEEKDAY, sum(sa.TOTAL_SALE) as Sale
-- from warehouse.products p, warehouse.dates d , warehouse.sales sa
-- where d.WEEKDAY in ("Sunday", "Saturday" )
-- and p.PRODUCT_ID = sa.PRODUCT_ID
-- and d.DATE_ID = sa.DATE_ID 
-- group by PRODUCT_ID
-- order by sale desc
-- limit 5;

-- Q4

-- select p.PRODUCT_ID, sum(sa.TOTAL_SALE) as Sale , d.QTR, d.YYYY
-- from warehouse.products p, warehouse.sales sa, warehouse.dates d
-- where d.YYYY = 2016
-- and sa.PRODUCT_ID = p.PRODUCT_ID 
-- and d.DATE_ID = sa.DATE_ID
-- group by d.QTR , PRODUCT_ID 
-- order by d.QTR;


-- Q5
-- -- first half(1) and second half (2)

-- select p.PRODUCT_ID, d.YYYY,  floor(d.QTR / 3) + 1 as half,  sum(sa.TOTAL_SALE) as sale
-- from warehouse.products p, warehouse.dates d, warehouse.sales sa
-- where p.PRODUCT_ID = sa.PRODUCT_ID
-- and d.DATE_ID = sa.DATE_ID
-- GROUP BY PRODUCT_ID,  (d.YYYY) + 0.1 * half;

-- -- total yearly sale
-- select p.PRODUCT_ID, d.YYYY, sum(sa.TOTAL_SALE) as sale
-- from warehouse.products p, warehouse.dates d, warehouse.sales sa
-- where d.YYYY = 2016
-- and p.PRODUCT_ID = sa.PRODUCT_ID
-- and d.DATE_ID = sa.DATE_ID
-- group by p.PRODUCT_ID, d.YYYY
-- order by p.PRODUCT_ID;


-- Q7

-- DROP TABLE if exists STOREANALYSIS_MV;
-- drop view if exists STOREANALYSIS_MV;

-- CREATE view STOREANALYSIS_MV as
--  SELECT s.STORE_ID, p.PRODUCT_ID, SUM(sa.TOTAL_SALE) as store_total
--  from warehouse.stores s, warehouse.sales sa, warehouse.products p
--  where
--  s.store_id = sa.store_id and
--  p.product_id = sa.product_id
--  GROUP by s.STORE_ID, p.PRODUCT_ID
--  ORDER by STORE_ID, PRODUCT_ID;
--  
--  select * from STOREANALYSIS_MV;
 
 
-- CREATE TABLE STOREANALYSIS_MV (
--     store_id VARCHAR(4)  NOT NULL
--   , product_id    VARCHAR(6) NOT NULL
--   , sale   decimal(10,2)          NOT NULL
-- );

-- INSERT INTO STOREANALYSIS_MV
-- SELECT s.STORE_ID, p.PRODUCT_ID, SUM(sa.TOTAL_SALE)
--  from warehouse.stores s, warehouse.sales sa, warehouse.products p
--  where s.store_id = sa.store_id and
--  p.product_id = sa.product_id
--  GROUP by s.STORE_ID, p.PRODUCT_ID
--  ORDER by STORE_ID, PRODUCT_ID;
--  
--  select * from STOREANALYSIS_MV;


