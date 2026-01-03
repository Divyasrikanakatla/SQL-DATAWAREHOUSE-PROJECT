/*
=============================================================
Database & Schema Setup
=============================================================
Purpose:
- Create the 'DataWarehouse' database
- Drop and recreate it if it already exists
- Create bronze, silver, and gold schemas 
*/
--Create Database
Use master
Create Database Datawarehouse;
Go

Use Datawarehouse;
GO
-- Create Schemas
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO

--To check whether schemas are created or not
select *from sys.schemas
where name IN('bronze','silver','gold')
GO
