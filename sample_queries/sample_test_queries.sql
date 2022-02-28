
--1. Check Row Count
SELECT
count(1)
FROM public.us_zipcodes;
--2. Check Duplicate rows in the Target
SELECT
count(1)
FROM public.us_zipcodes GROUP BY zip having count(1)>1 limit 10;
--3. No of unique Zipcodes in USA
SELECT COUNT(DISTINCT Zip) FROM public.us_zipcodes;
--4. Number of rows Inserted

--5. Number of rows Updated

