# CSV Database Importer - Configuration Guide

This document provides detailed instructions on how to configure and optimize the CSV Database Importer application for your specific needs.

## Table of Contents
1. [Initial Setup](#initial-setup)
2. [Configuration File](#configuration-file)
3. [Database Setup](#database-setup)
4. [CSV File Requirements](#csv-file-requirements)
5. [Performance Tuning](#performance-tuning)
6. [Advanced Configuration](#advanced-configuration)
7. [Logging Configuration](#logging-configuration)
8. [Troubleshooting](#troubleshooting)

## Initial Setup

Before running the application, you need to:

1. **Create Required Folders**:
   - Create a folder for your CSV files
   - Create a folder for logs

2. **Set Up the Database**:
   - Ensure your SQL Server instance is running
   - Create a database if it doesn't exist
   - Create a destination table with the appropriate schema

3. **Prepare Configuration File**:
   - Modify `config.json` in the `Configuration` folder with your settings
   - Ensure all paths exist and are accessible

## Configuration File

The `config.json` file is located in the `Configuration` folder and contains settings organized into three sections:

### Complete Sample Configuration
```json
{
  "DatabaseConfig": {
    "Server": "[YourServerName]",
    "Database": "[YourDatabaseName]",
    "IntegratedSecurity": true,
    "Username": null,
    "Password": null
  },
  "ProcessConfig": {
    "CsvFolderPath": "[PathToYourCSVFolder]",
    "TempTableName": "IMP_TEMP_TABLE",
    "DestinationTableName": "[YourDestinationTable]",
    "ErrorTableName": "CSV_IMPORT_ERRORS",
    "SuccessLogTableName": "CSV_IMPORT_SUCCESS",
    "BatchSize": 100000,
    "ValidateColumnMapping": true,
    "AutoExitTimeoutSeconds": 10,
    "PreserveLeadingZeros": true
  },
  "LoggingConfig": {
    "LogFolderPath": "[PathToYourLogFolder]",
    "EnableFileLogging": true,
    "ErrorLogFolder": "Errors",
    "SuccessLogFolder": "Success",
    "ConsoleLogFolder": "Console"
  }
}
```

### Detailed Parameter Explanations

#### DatabaseConfig
| Parameter | Description | Example | Notes |
|-----------|-------------|---------|-------|
| Server | SQL Server instance name | "ServerName" or "localhost\\SQLEXPRESS" | Use local instance name or network path |
| Database | Target database name | "YourDatabase" | Must exist before running the importer |
| IntegratedSecurity | Use Windows authentication | true | Set to false for SQL Server authentication |
| Username | SQL Server login | "username" | Only used if IntegratedSecurity is false |
| Password | SQL Server password | "password" | Only used if IntegratedSecurity is false |

#### ProcessConfig
| Parameter | Description | Example | Notes |
|-----------|-------------|---------|-------|
| CsvFolderPath | Path to folder with CSV files | "C:\\Path\\To\\CSVFiles" | Must exist and be accessible |
| TempTableName | Temporary table name | "IMP_TEMP_TABLE" | Will be created and dropped automatically |
| DestinationTableName | Target table for data | "YourTargetTable" | Must exist with compatible schema |
| ErrorTableName | Error logging table | "CSV_IMPORT_ERRORS" | Created automatically if it doesn't exist |
| SuccessLogTableName | Success logging table | "CSV_IMPORT_SUCCESS" | Created automatically if it doesn't exist |
| BatchSize | Rows to process per batch | 100000 | Adjust based on available memory |
| ValidateColumnMapping | Check column compatibility | true | Ensures CSV columns match destination |
| AutoExitTimeoutSeconds | Time before auto-exit | 10 | Set to 0 to disable auto-exit |
| PreserveLeadingZeros | Keep leading zeros in numbers | true | Important for codes, ZIP codes, etc. |

#### LoggingConfig
| Parameter | Description | Example | Notes |
|-----------|-------------|---------|-------|
| LogFolderPath | Base path for all logs | "C:\\Path\\To\\Logs" | Must exist and be writable |
| EnableFileLogging | Enable/disable file logging | true | Set to false for console-only logging |
| ErrorLogFolder | Subfolder for error logs | "Errors" | Created automatically |
| SuccessLogFolder | Subfolder for success logs | "Success" | Created automatically |
| ConsoleLogFolder | Subfolder for console logs | "Console" | Created automatically |

## Database Setup

### Required Tables

#### Destination Table
Create your destination table with columns that match your CSV file:

```sql
CREATE TABLE [YourDestinationTable] (
    -- Add columns to match your CSV structure
    -- Example:
    ID INT,
    Name NVARCHAR(255),
    Date DATE,
    Value DECIMAL(18,2),
    Code NVARCHAR(50)  -- Use NVARCHAR for fields with leading zeros
);
```

#### Error Logging Table
This table will be created automatically but can be pre-created:

```sql
CREATE TABLE CSV_IMPORT_ERRORS (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FileName NVARCHAR(MAX),
    ColumnName NVARCHAR(MAX),
    ErrorType NVARCHAR(100),
    Reason NVARCHAR(MAX),
    SourceValue NVARCHAR(MAX),
    DestinationValue NVARCHAR(MAX),
    Timestamp DATETIME DEFAULT GETDATE()
);
```

#### Success Logging Table
This table will be created automatically but can be pre-created:

```sql
CREATE TABLE CSV_IMPORT_SUCCESS (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Message NVARCHAR(MAX),
    TotalRows BIGINT,
    SourceColumns INT,
    DestinationColumns INT,
    MatchedColumns INT,
    ProcessingTimeSeconds DECIMAL(18,2),
    RowsPerSecond INT,
    Timestamp DATETIME DEFAULT GETDATE()
);
```

### Database User Permissions
The user specified in the configuration needs the following permissions:
- CREATE TABLE (for creating temporary and log tables)
- INSERT, SELECT, UPDATE, DELETE on the destination table
- If using integrated security, the Windows user running the application must have these permissions

## CSV File Requirements

### File Format
- Files must be valid CSV format with consistent delimiters
- Comma (,) is the expected delimiter
- First row must contain column headers
- Column headers should match destination table column names for automatic mapping
- Quote handling is supported for fields containing commas

### File Naming
- Files are processed in alphabetical order
- Use a consistent naming convention (e.g., IMPORT-YYYYMMDD-NN.csv)
- The first file will truncate the destination table if configured

### Special Data Handling
- Leading zeros are preserved when PreserveLeadingZeros is set to true
- Dates should be in one of the supported formats: dd-MM-yyyy, MM/dd/yyyy, yyyy-MM-dd
- Text fields with special characters should be properly quoted

## Performance Tuning

### Memory Optimization
- **BatchSize**: Adjust based on available system memory
  - Lower values (e.g., 10,000) for systems with limited memory
  - Higher values (e.g., 500,000) for systems with abundant memory

### SQL Server Optimization
- Disable indexes on the destination table before import, then rebuild after
- Consider using a simple recovery model for the database during imports
- Ensure adequate transaction log space is available

### File Processing
- Split very large files into smaller files for better manageability
- Processing multiple smaller files is often more efficient than one large file
- Ensure CSV files are not open in other applications during import

## Advanced Configuration

### Command Line Execution
You can run the application via command line:

```
CSVDatabaseImporter.exe
```

For automated execution, consider using Windows Task Scheduler with a batch file:

```batch
@echo off
cd /d "[PathToApplicationFolder]"
CSVDatabaseImporter.exe
```

### Configuration for Very Large Files
For extremely large files (hundreds of millions of rows), consider:

```json
"ProcessConfig": {
  "BatchSize": 50000,
  "ValidateColumnMapping": true,
  "AutoExitTimeoutSeconds": 0
}
```

### Multiple Server Environments
Create different config files for different environments:

- `config.dev.json` - Development environment
- `config.prod.json` - Production environment

Copy the appropriate file to `config.json` before running.

## Logging Configuration

### Console Logging
- Color-coded for different message types:
  - Cyan: Information messages
  - Magenta: Configuration information
  - Green: Success messages
  - Yellow: Warning messages
  - Red: Error messages
  - DarkGreen: Progress updates
  - Blue: Database operations

### File Logging
- Each run creates timestamp-named log files
- Log files are organized in subfolders by type

### Minimal Logging Configuration
For minimal logging (useful in automated scenarios):

```json
"LoggingConfig": {
  "LogFolderPath": "[PathToLogFolder]",
  "EnableFileLogging": true,
  "ErrorLogFolder": "Errors",
  "SuccessLogFolder": "Success",
  "ConsoleLogFolder": "Console"
}
```

## Troubleshooting

### Common Errors and Solutions

#### Error: Unable to open CSV file
- Ensure the file exists at the specified path
- Check file permissions
- Verify the file is not open in another application

#### Error: Failed to connect to database
- Confirm SQL Server is running
- Verify connection string parameters
- Check network connectivity for remote servers
- Ensure SQL authentication credentials are correct

#### Error: Column validation failed
- CSV columns must match destination table columns
- Check for case sensitivity or spacing issues in column names
- Set ValidateColumnMapping to false if you want to ignore mismatches

#### Error: Row count mismatch
- Check for constraint violations in the destination table
- Ensure destination table doesn't have triggers that filter rows
- Check for unique key constraints that might prevent duplicates

### Diagnostic Queries
Query the error log table for specific issues:

```sql
-- Get the most recent errors
SELECT TOP 100 * 
FROM CSV_IMPORT_ERRORS 
ORDER BY Timestamp DESC;

-- Get errors for a specific file
SELECT * 
FROM CSV_IMPORT_ERRORS 
WHERE FileName = 'YourFileName.csv' 
ORDER BY Timestamp DESC;

-- Get success records
SELECT * 
FROM CSV_IMPORT_SUCCESS 
ORDER BY Timestamp DESC;
```

### Log File Analysis
- Check the error log files for detailed error messages
- Console logs contain a complete record of all operations
- Timestamps in logs can help identify bottlenecks

If you continue to experience issues, please check the detailed application logs or contact technical support.