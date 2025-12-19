-- Create a new database--

use master;
GO
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE name = 'datawarehouse')
BEGIN 
	ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE datawarehouse
END

CREATE DATABASE datawarehouse;
GO

USE datawarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
