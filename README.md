# newsela poc

ETL
US ZipCode data was taken in order to create this ETL poc.

https://simplemaps.com/data/us-zips


1. Create two Postgres Tables:

A. public.staging_us_zipcodes
B. public.us_zipcodes

2. Created a Python based ETL process.

Steps Followed:
a. Truncate Stage table, public.staging_us_zipcodes.
b. Load from CSV file to public.staging_us_zipcodes, this table would carry raw data.
c. Transform the data in 'public.staging_us_zipcodes' table, cleanse and load into target table, 'public.us_zipcodes'.
d. Perform Merge operation, by deleting all records modified in Source.
Option-1: Delete from Target - Brut force
Option-2: Compare the data between the Stage and Target and delete if the data is identified as modified from source.

e. Insert all records from Stage which includes all New records and Modified records.

Note: Above steps can be slightly modified if the table should be loaded in SCD Type-2 fashion.


3. Natice that there are no duplicates in the target.

