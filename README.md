**SQL Server Data Warehouse – Bronze, Silver & Gold Architecture**

🚀 Project Overview:
         
 This repository showcases a production-style SQL Server Data Warehouse implemented using the Medallion Architecture (Bronze, Silver, Gold). 
 The project demonstrates end-to-end data engineering skills including raw data ingestion, transformation, data quality enforcement, and analytical modeling. 
 The solution integrates CRM and ERP source systems, processes them using T-SQL stored procedures, and exposes analytics-ready views suitable for BI tools.
         
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

🏗 ️Architecture:
        
         CSV Files
           │
           ▼
         Bronze Layer (Raw Ingestion)
           │
           ▼
         Silver Layer (Cleaned & Conformed)
           │
           ▼
         Gold Layer (Analytics / Star Schema)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         
 
 🥉 Bronze Layer:
        
        • Raw ingestion from CSV files using   _BULK INSERT_
        • Minimal transformations
        • Full reload strategy using   _TRUNCATE + INSERT_
        
        **Stored procedure: bronze.load_bronze**
        
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------        

🥈 Silver Layer:
        
        • Data cleansing and standardization
        • Deduplication using  _ROW_NUMBER()_ window functions
        • Business rule enforcement (dates, gender, sales validation)
        • Audit columns for lineage (dwh_create_date) --------------- [meta data creation]
        
        **Stored procedure: silver.load_silver**
        
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

🥇 Gold Layer:
       
        • Analytics-ready star schema implemented as  _views_ 
        • Surrogate keys generated with  _ROW_NUMBER()_
        • Fact and dimension separation
        • Optimized for reporting and BI tools

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------        

⚙ ETL Stored Procedure Development - Medallion Architecture Implementation
    
Engineered comprehensive T-SQL stored procedures (bronze.load_bronze & silver.load_silver) for a production-grade SQL Server data warehouse implementing the Medallion Architecture (Bronze → Silver → Gold layers).

    🥉 _**Bronze Layer Automation (bronze.load_bronze):**_
        
• Orchestrated high-performance BULK INSERT operations across 6 CSV sources (CRM/ERP) with optimized FIRSTROW=2, FIELDTERMINATOR=',', and TABLOCK for minimal locking.

• Implemented atomic TRUNCATE + RELOAD pattern with comprehensive performance telemetry using GETDATE()/DATEDIFF(SECOND) logging.

• Enterprise-grade TRY-CATCH error handling capturing ERROR_MESSAGE(), ERROR_NUMBER(), and ERROR_STATE().

    🥈 **_Silver Layer Transformation (silver.load_silver):_**

• Executed data cleansing pipeline with TRIM(), UPPER(), and conditional CASE logic for standardization.

• Applied deduplication via ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) for SCD Type 2 latest records.

• Complex string parsing (SUBSTRING, REPLACE) and data validation (price integrity: sls_sales = quantity × ABS(price)).

• Temporal transformations converting integer date codes to DATE format with null-handling logic.

    _**Key Engineering Practices:**_

• Production logging framework tracking per-table and batch-level execution durations.

• Schema evolution readiness with CREATE OR ALTER PROCEDURE.

• Star Schema foundation enabling downstream gold.dim_customers, gold.dim_products, and gold.fact_sales views.

📌 Performance & Scalability: 
        
Demonstrates intermediate Data Engineering proficiency suitable for enterprise ETL orchestration, processing multi-source CRM/ERP datasets into BI-ready semantic models.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

� Repository Structure:
            
             sql-server-data-warehouse/
             │
             ├── README.md
             │
             ├── sql/
             │   ├── 01_database_setup.sql
             │   ├── 02_bronze_layer.sql
             │   ├── 03_silver_layer.sql
             │   └── 04_gold_layer.sql
             │
             ├── datasets/
             │   ├── source_crm/
             │   │   ├── cust_info.csv
             │   │   ├── prd_info.csv
             │   │   └── sales_details.csv
             │   └── source_erp/
             │       ├── cust_az12.csv
             │       ├── loc_a101.csv
             │       └── px_cat_g1v2.csv
             │
             └── docs/
                ├── architecture.png
                └── data-model.png
                
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

⚙Technologies Used:
        
        • SQL Server
        • T-SQL
        • Stored Procedures
        • Views
        • Window Functions
        • BULK INSERT
        • Star Schema Modeling
        
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

▶ How to Run the Project:

1. Clone the repository 
2. Open SQL Server Management Studio (SSMS) 
3. Execute scripts in order:
                01_database_setup.sql
                02_bronze_layer.sql
                03_silver_layer.sql
                04_gold_layer.sql
4. Run the ETL pipelines:

        **EXEC bronze.load_bronze;**
        **EXEC silver.load_silver;**

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

📊 Gold Layer Objects:

        Dimensions: 
            • _gold.dim_customers _
            • _gold.dim_products_
    
        Fact Table:
            • _gold.fact_sales _

     These views are designed for direct consumption by BI and analytics tools.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

✅ Key Features:
        
        • Idempotent ETL pipelines
        • Clear separation of concerns across layers
        • Real-world data quality handling
        • Analytics-ready dimensional modeling
        • Implement-ready, production-style SQL project

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

📌 Future Enhancements:
 
        • Incremental loading
        • Error and audit logging tables
        • SQL Agent scheduling
        • Indexing and performance tuning
        • Slowly Changing Dimensions (SCD Type 2)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         

👤 Author
        
        Hari Vikas M
        Business Operations Associate | SQL Server | Analytics Engineering
        
⭐ If you find this project useful, feel free to star the repository!

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------         
