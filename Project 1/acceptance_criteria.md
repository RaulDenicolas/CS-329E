Requirement 1: Five unique entities
The Minerals (mineral_ores_around_the_world) dataset was uploaded to our Bucket from the published dataset on Kaggle: https://www.kaggle.com/datasets/ramjasmaurya/mineral-ores-around-the-world
Site_name - Name of Site (STRING)
Latitude - Latitude of Site (STRING)
Longitude - Longitude (STRING)
Country - Country of Site  (STRING)
State - State of Site  (STRING)
County - County of Site (STRING)
Com_type - Compound Type of Minerals (STRING)
Commod1 - First Mineral Found (STRING)
Commod2 - Second Mineral Found (STRING
Commod3 - Third Mineral Found (STRING)

The Mineral Ore Sites (mrds) dataset was uploaded to our Bucket from the published dataset on USGS: https://mrdata.usgs.gov/mrds/
Dep_id - Deposit Identification Number (STRING)
Mrds_id - Mineral Resource Data System (MRDS) Identification number (STRING)
Mas_id - MAS/MILS Identification Number (STRING)
Site_name - Name of the site, deposit, or operation (STRING)
Latitude - Latitude (STRING)
Longitude - Longitude (STRING)
Country - Country Name (STRING)
State - State (STRING)
County - County (STRING)
Com_type - Commodity type (STRING)
Commod1 - Primary commodities (STRING)
Commod2 - Secondary commodities (STRING)
Commod3 - Tertiary commodities (STRING)

The dataset Sales uploaded to BQ is from the Retail Data Analytics dataset from 2011 published on Kaggle: https://www.kaggle.com/datasets/manjeetsingh/retaildataset
Focusing on the sales data-set.csv 
The unique entities in this table are expressed as: 
• Store - the store number (INTEGER) 
• Dept - the department number (INTEGER)
• Date - the week (DATE) 
• Weekly_Sales -  sales for the given department in the given store (FLOAT) 
• IsHoliday - whether the week is a special holiday week (Boolean)  
These entities meet the minimum requirement of 5 explained in criteria one. 

The data set, train.csv, I uploaded to BQ is from the Stores Sales- Time Series Forecasting also uploaded from Kaggle: https://www.kaggle.com/competitions/store-sales-time-series-forecasting/overview,  of which the dataset is training data for sales of the thousands of product families sold at Favorita stores located in Ecuador. The unique entities in this table train.csv are expressed as:
The dataset, comprises time series of features store_nbr, family, and onpromotion as well as the target sales and calls this entry date. (DATE)
store_nbr identifies the store at which the products are sold. (INTEGER) 
family identifies the type of product sold. (STRING)
sales gives the total sales for a product family at a particular store at a given date. Fractional values are possible since products can be sold in fractional units (1.5 kg of cheese, for instance, as opposed to 1 bag of chips). (FLOAT)
onpromotion gives the total number of items in a product family that was being promoted at a store on a given date. (INTEGER)

Bird Data tables from the database Retails 
Parts (part)
p_partkey
p_type
p_size
p_brand
p_name
p_container
p_mfgr
p_retailprice
p_comment

Parts Details (partsupp)
ps_partkey
ps_suppkey
ps_supplycost
ps_availqty
ps_comment

Suppliers (supplier)
s_suppkey
s_nationkey
s_comment
s_name
s_address
s_phone
S_acctbal
Requirement 2: Sources of Data
Entities parts, partsupp, and supplier were sourced from BIRD
Entity mineral_ores_around_the_world, sales_data_set, and stores_sales_data_set were sourced from Kaggler
Entities mrds was sources from the United States Geological Survey
Entities train was sourced from Kaggle 
Entities from the sales dataset was sourced from kaggle
Requirement 3: Functional Dependencies
Functional dependencies can be expressed as this: 
mrds:
mrds.county -> mrds.state -> mrds.country
A site’s country is depended on the state and country of a site
mineral_ores_around_the_world:
mineral_ores_around_the_world.state -> mineral_ores_around_the_world.country
A site’s country is depended on the state of a site
From the sales dataset: 
- Date ->IsHoliday 
Date can determine if the date was indeed a holiday 
-Store,Dept,Date->Weekly_Sales
Weekly_sales is varied amongst the different combinations of the store,department and date.
From the train dataset: 
-Family,onpromotion -> sales 
Identifier of the type of product sold and the influence of a promotion of an item can vary the sales
Requirement 4: Column Storing More Than One Column
supplier: supplier.comment
partsupp: partsupp.comment
parts: parts.comment
Requirement 5:
The attributes of these tables, (store data-set.csv ) you would expect to see these in a retail related dataset, whereas the attributes of our raw data (retails.xlsx) also indicates attributes of a retail store but some attributes focus in more on the merchandize. You see this with keys such as “brand”, “retailprice”, “container” and “size”. .  

Requirement 6:  Raw Table Representing > One Entity
Our Raw Tables are
Parts.csv 
Partsupp.csv
Supplier.csv 

There is no direct match between the entities described in the sales_data_set.csv and train.csv the tables in our raw data. They represent different aspects of a business operation—sales data entities are related to retail operations, while our raw data entities are related to supply chain and inventory.


Requirement 7: Two Disjoint Entities
mineral_ores_around_the_world and mrds: can be joined through an Ores Needed For Parts table. This is not represented in the data

