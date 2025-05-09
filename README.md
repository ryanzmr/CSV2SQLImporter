# CSV Database Importer

A high-performance utility for importing CSV files into SQL Server databases with support for configuration, logging, and error handling.

## Overview

CSV Database Importer is a command-line utility designed to efficiently transfer data from CSV files to SQL Server tables. It features robust error handling, detailed logging, performance optimization, and support for preserving leading zeros in numeric fields.

## Features

- **High Performance**: Optimized for processing large CSV files with millions of rows
- **Configurable**: Uses JSON-based configuration to customize all aspects of operation
- **Robust Error Handling**: Detailed error logging and graceful recovery
- **Progress Tracking**: Real-time progress updates during processing
- **Data Type Preservation**: Special handling for preserving leading zeros
- **Validation**: Column mapping validation ensures data integrity
- **Colorful Console Output**: Color-coded logging for better readability
- **Execution Summary**: Detailed performance statistics after completion

## Installation

1. Download the latest release
2. Extract the files to a location on your computer
3. Modify the `config.json` file to match your environment
4. Run `CSVDatabaseImporter.exe`

## Configuration

The application uses a JSON configuration file (`config.json`) located in the `Configuration` folder. The following settings are available:

### Database Configuration

```json
"DatabaseConfig": {
  "Server": "[YourServerName]",
  "Database": "[YourDatabaseName]",
  "IntegratedSecurity": true,
  "Username": null,
  "Password": null
}
```

- `Server`: SQL Server instance name
- `Database`: Target database name
- `IntegratedSecurity`: Use Windows authentication (true) or SQL Server authentication (false)
- `Username`: SQL Server login (only used if IntegratedSecurity is false)
- `Password`: SQL Server password (only used if IntegratedSecurity is false)

### Process Configuration

```json
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
}
```

- `CsvFolderPath`: Path to folder containing CSV files to import
- `TempTableName`: Name of temporary table used during import process
- `DestinationTableName`: Target table where data will be inserted
- `ErrorTableName`: Table for storing error information
- `SuccessLogTableName`: Table for storing successful import information
- `BatchSize`: Number of rows to process in each batch
- `ValidateColumnMapping`: Validate columns match between CSV and destination table
- `AutoExitTimeoutSeconds`: Seconds to wait before automatically closing the application
- `PreserveLeadingZeros`: Whether to preserve leading zeros in numeric fields

### Logging Configuration

```json
"LoggingConfig": {
  "LogFolderPath": "[PathToYourLogFolder]",
  "EnableFileLogging": true,
  "ErrorLogFolder": "Errors",
  "SuccessLogFolder": "Success",
  "ConsoleLogFolder": "Console"
}
```

- `LogFolderPath`: Base path for all log files
- `EnableFileLogging`: Enable or disable file logging
- `ErrorLogFolder`: Subfolder for error logs
- `SuccessLogFolder`: Subfolder for success logs
- `ConsoleLogFolder`: Subfolder for console output logs

## Usage

1. Place your CSV files in the configured `CsvFolderPath` folder
2. Ensure the target database and table exist with compatible schema
3. Run the application by executing `CSVDatabaseImporter.exe`
4. The application will process all CSV files in the specified folder in alphabetical order
5. Progress and results will be displayed in the console
6. After completion, the application will show a detailed summary and exit automatically

## Example config.json

```json
{
  "DatabaseConfig": {
    "Server": "localhost\\SQLEXPRESS",
    "Database": "ImportDatabase",
    "IntegratedSecurity": true,
    "Username": null,
    "Password": null
  },
  "ProcessConfig": {
    "CsvFolderPath": "C:\\Import\\CSV",
    "TempTableName": "IMP_TEMP_TABLE",
    "DestinationTableName": "ImportedData",
    "ErrorTableName": "CSV_IMPORT_ERRORS",
    "SuccessLogTableName": "CSV_IMPORT_SUCCESS",
    "BatchSize": 100000,
    "ValidateColumnMapping": true,
    "AutoExitTimeoutSeconds": 10,
    "PreserveLeadingZeros": true
  },
  "LoggingConfig": {
    "LogFolderPath": "C:\\Import\\Logs",
    "EnableFileLogging": true,
    "ErrorLogFolder": "Errors",
    "SuccessLogFolder": "Success",
    "ConsoleLogFolder": "Console"
  }
}
```

## Requirements

- Windows operating system
- .NET 8.0 Runtime or SDK
- SQL Server 2016 or later
- Sufficient permissions to read CSV files and write to SQL Server

## Logging and Error Handling

- All operations are logged to the console with color-coding
- Errors are captured and stored in the `ErrorTableName` table
- Successful imports are recorded in the `SuccessLogTableName` table
- Console output is saved to a file if `EnableFileLogging` is true

## Performance Optimization

The application includes several performance optimizations:

- Batch processing of CSV files
- Memory caching for frequently parsed values
- Optimized database operations with table hints
- Configurable batch size for optimal memory usage
- Efficient bulk data loading

## Troubleshooting

### Common Issues

1. **File not found errors**: Ensure the CSV folder path is correct and accessible
2. **Database connection errors**: Verify SQL Server is running and credentials are correct
3. **Column mapping errors**: Ensure CSV columns match destination table columns
4. **Permission issues**: Check file system and database permissions

### Error Log Location

Error logs are stored in:
- Database: In the configured `ErrorTableName` table
- File system: In the `LogFolderPath/ErrorLogFolder` directory

## Support

For support or questions, please file an issue on the project's issue tracker.

## License

[MIT License](LICENSE)