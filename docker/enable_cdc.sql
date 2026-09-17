-- ==============================================================================
-- Enable Change Data Capture (CDC) for AdventureWorks
-- ==============================================================================

USE AdventureWorks;
GO

-- 1. Enable CDC at Database level
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'AdventureWorks' AND is_cdc_enabled = 1)
BEGIN
    PRINT 'Enabling CDC on database AdventureWorks...';
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on database AdventureWorks successfully.';
END
ELSE
BEGIN
    PRINT 'CDC is already enabled on database AdventureWorks.';
END
GO

-- 2. Ensure Primary Keys on all tables (required for CDC net changes)

-- -- calendar
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_calendar')
-- BEGIN
--     ALTER TABLE [calendar] ALTER COLUMN [date] DATETIME NOT NULL;
--     ALTER TABLE [calendar] ADD CONSTRAINT PK_calendar PRIMARY KEY ([date]);
-- END
-- GO

-- customers
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_customers')
BEGIN
    ALTER TABLE [customers] ALTER COLUMN [customer_key] BIGINT NOT NULL;
    ALTER TABLE [customers] ADD CONSTRAINT PK_customers PRIMARY KEY ([customer_key]);
END
GO

-- -- product_categories
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_product_categories')
-- BEGIN
--     ALTER TABLE [product_categories] ALTER COLUMN [product_category_key] BIGINT NOT NULL;
--     ALTER TABLE [product_categories] ADD CONSTRAINT PK_product_categories PRIMARY KEY ([product_category_key]);
-- END
-- GO

-- -- product_subcategories
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_product_subcategories')
-- BEGIN
--     ALTER TABLE [product_subcategories] ALTER COLUMN [product_subcategory_key] BIGINT NOT NULL;
--     ALTER TABLE [product_subcategories] ADD CONSTRAINT PK_product_subcategories PRIMARY KEY ([product_subcategory_key]);
-- END
-- GO

-- -- products
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_products')
-- BEGIN
--     ALTER TABLE [products] ALTER COLUMN [product_key] BIGINT NOT NULL;
--     ALTER TABLE [products] ADD CONSTRAINT PK_products PRIMARY KEY ([product_key]);
-- END
-- GO

-- -- territories
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_territories')
-- BEGIN
--     ALTER TABLE [territories] ALTER COLUMN [sales_territory_key] BIGINT NOT NULL;
--     ALTER TABLE [territories] ADD CONSTRAINT PK_territories PRIMARY KEY ([sales_territory_key]);
-- END
-- GO

-- -- returns
-- IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_returns')
-- BEGIN
--     ALTER TABLE [returns] ALTER COLUMN [return_date] DATETIME NOT NULL;
--     ALTER TABLE [returns] ALTER COLUMN [territory_key] BIGINT NOT NULL;
--     ALTER TABLE [returns] ALTER COLUMN [product_key] BIGINT NOT NULL;
--     ALTER TABLE [returns] ADD CONSTRAINT PK_returns PRIMARY KEY ([return_date], [territory_key], [product_key]);
-- END
-- GO

-- sales
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_sales')
BEGIN
    ALTER TABLE [sales] ALTER COLUMN [order_number] VARCHAR(50) NOT NULL;
    ALTER TABLE [sales] ALTER COLUMN [order_line_item] BIGINT NOT NULL;
    ALTER TABLE [sales] ADD CONSTRAINT PK_sales PRIMARY KEY ([order_number], [order_line_item]);
END
GO

-- 3. Enable CDC for each table
DECLARE @tables TABLE (table_name NVARCHAR(100));
INSERT INTO @tables VALUES 
    -- ('calendar'),
    ('customers'),
    -- ('product_categories'),
    -- ('product_subcategories'),
    -- ('products'),
    -- ('returns'),
    ('sales');
    -- ('territories');

DECLARE @tbl NVARCHAR(100);
DECLARE cur CURSOR FOR SELECT table_name FROM @tables;

OPEN cur;
FETCH NEXT FROM cur INTO @tbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        WHERE t.name = @tbl AND t.is_tracked_by_cdc = 1
    )
    BEGIN
        PRINT 'Enabling CDC on table: ' + @tbl;
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name   = @tbl,
            @role_name     = NULL,
            @supports_net_changes = 1;
        PRINT 'CDC enabled on: ' + @tbl;
    END
    ELSE
    BEGIN
        PRINT 'CDC already enabled on: ' + @tbl;
    END

    FETCH NEXT FROM cur INTO @tbl;
END

CLOSE cur;
DEALLOCATE cur;
GO

-- 4. Check CDC status summary
SELECT 
    s.name AS [schema_name],
    t.name AS [table_name],
    t.is_tracked_by_cdc,
    ct.name AS [cdc_change_table]
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN cdc.change_tables c ON c.source_object_id = t.object_id
LEFT JOIN sys.tables ct ON c.capture_instance = ct.name
WHERE s.name = 'dbo'
ORDER BY t.name;
GO
