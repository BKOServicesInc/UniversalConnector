-- SQL Server initialisation for UniversalConnector
-- Runs once via the custom entrypoint after sqlservr is ready.
--
-- What this does:
--   1. Creates the crmdb database.
--   2. Enables Change Tracking on the database (no SQL Agent required).
--   3. Creates the connector login + user with appropriate permissions.
--   4. Creates seed tables and enables Change Tracking per-table.
--   5. Inserts seed rows for smoke testing.

USE master;
GO

-- ── 1. Database ───────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'crmdb')
BEGIN
    CREATE DATABASE crmdb;
    PRINT 'Database crmdb created.';
END
GO

-- ── 2. Enable Change Tracking on the database ─────────────────────────────────
-- CHANGE_RETENTION: how long SQL Server keeps change history (used for polling)
-- AUTO_CLEANUP:     purge expired records automatically
ALTER DATABASE crmdb
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

USE crmdb;
GO

-- ── 3. Connector login and user ───────────────────────────────────────────────
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = N'connector')
BEGIN
    -- Password must match SQLSERVER_PASSWORD env var; default matches .env.example
    CREATE LOGIN connector WITH PASSWORD = '$(SQLSERVER_PASSWORD)';
    PRINT 'Login connector created.';
END
GO

IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = N'connector')
BEGIN
    CREATE USER connector FOR LOGIN connector;
    PRINT 'User connector created.';
END
GO

-- Minimal permissions: read data + query change tracking metadata
ALTER ROLE db_datareader ADD MEMBER connector;
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO connector;
GO

-- ── 4. Tables and per-table Change Tracking ───────────────────────────────────
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name = 'dbo' AND t.name = 'Customers'
)
BEGIN
    CREATE TABLE dbo.Customers (
        CustomerId   INT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
        FirstName    NVARCHAR(100) NOT NULL,
        LastName     NVARCHAR(100) NOT NULL,
        Email        NVARCHAR(255)     NULL,
        ModifiedDate DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    ALTER TABLE dbo.Customers
        ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

    PRINT 'Table Customers created with Change Tracking.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name = 'dbo' AND t.name = 'Contacts'
)
BEGIN
    CREATE TABLE dbo.Contacts (
        ContactId    INT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
        CustomerId   INT               NULL REFERENCES dbo.Customers(CustomerId),
        PhoneNumber  NVARCHAR(50)      NULL,
        ContactType  NVARCHAR(50)  NOT NULL DEFAULT 'mobile',
        ModifiedDate DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    ALTER TABLE dbo.Contacts
        ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);

    PRINT 'Table Contacts created with Change Tracking.';
END
GO

-- ── 5. Seed data ──────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM dbo.Customers)
BEGIN
    INSERT INTO dbo.Customers (FirstName, LastName, Email)
    VALUES ('Alice',   'Smith',   'alice@example.com'),
           ('Bob',     'Jones',   'bob@example.com'),
           ('Charlie', 'Brown',   NULL);

    INSERT INTO dbo.Contacts (CustomerId, PhoneNumber, ContactType)
    VALUES (1, '+1-555-0101', 'mobile'),
           (2, '+1-555-0202', 'work');

    PRINT 'Seed data inserted.';
END
GO
