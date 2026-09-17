-- ==============================================================================
-- Enable Change Data Capture (CDC) for AdventureWorks
-- Specifically configured for 'products' and 'sales' tables
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

-- 2. Ensure Primary Keys on tables (required for CDC net changes)

-- products
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_products')
BEGIN
    PRINT 'Setting product_key to NOT NULL...';
    ALTER TABLE [dbo].[products] ALTER COLUMN [product_key] BIGINT NOT NULL;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_products')
BEGIN
    PRINT 'Adding Primary Key constraint PK_products...';
    ALTER TABLE [dbo].[products] ADD CONSTRAINT PK_products PRIMARY KEY ([product_key]);
END
GO

-- sales
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_sales')
BEGIN
    PRINT 'Setting order_number and order_line_item to NOT NULL...';
    ALTER TABLE [dbo].[sales] ALTER COLUMN [order_number] VARCHAR(50) NOT NULL;
    ALTER TABLE [dbo].[sales] ALTER COLUMN [order_line_item] BIGINT NOT NULL;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_sales')
BEGIN
    PRINT 'Adding Primary Key constraint PK_sales...';
    ALTER TABLE [dbo].[sales] ADD CONSTRAINT PK_sales PRIMARY KEY ([order_number], [order_line_item]);
END
GO

-- (Optional: PK definitions for other tables kept for reference)
-- customers:
-- ALTER TABLE [customers] ALTER COLUMN [customer_key] BIGINT NOT NULL;
-- ALTER TABLE [customers] ADD CONSTRAINT PK_customers PRIMARY KEY ([customer_key]);

-- calendar:
-- ALTER TABLE [calendar] ALTER COLUMN [date] DATETIME NOT NULL;
-- ALTER TABLE [calendar] ADD CONSTRAINT PK_calendar PRIMARY KEY ([date]);


-- 3. Enable CDC for each target table
DECLARE @tables TABLE (table_name NVARCHAR(100));
INSERT INTO @tables VALUES 
    ('products'),
    ('sales');

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
    c.capture_instance,
    ct.name AS [cdc_change_table]
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN cdc.change_tables c ON c.source_object_id = t.object_id
LEFT JOIN sys.tables ct ON c.capture_instance = ct.name
WHERE s.name = 'dbo' AND t.name IN ('products', 'sales')
ORDER BY t.name;
GO
