
/*
Delete rows if the data already exists
Insert All New and Modified rows from Source (public.staging_us_zipcodes) to Target (public.us_zipcodes):

*/
DELETE FROM public.us_zipcodes
USING public.staging_us_zipcodes WHERE public.us_zipcodes.zip = public.staging_us_zipcodes.zip;

INSERT INTO public.us_zipcodes
(zip, lat, lng, city, state_id, state_name, zcta, parent_zcta, population, density, county_fips, county_name, county_weights,
county_names_all, county_fips_all,
 imprecise,military, timezone )
SELECT 
New.zip
 ,New.lat
 ,New.lng
 ,New.city
 ,New.state_id
 ,New.state_name
 ,New.zcta
 ,New.parent_zcta
 ,New.population
 ,New.density
 ,New.county_fips
 ,New.county_name
 ,New.county_weights
 ,New.county_names_all
 ,New.county_fips_all
 ,New.imprecise
 ,New.military
 ,New.timezone 
FROM public.staging_us_zipcodes AS New
LEFT JOIN public.us_zipcodes AS cur
ON New.zip = New.zip
WHERE cur.zip IS NULL;


