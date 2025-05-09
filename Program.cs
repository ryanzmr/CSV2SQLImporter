using System.Diagnostics;
using CSVDatabaseImporter.Models;
using CSVDatabaseImporter.Services;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter
{
    internal static class Program
    {
        private static readonly Stopwatch _totalExecutionStopwatch = new();
        
        private static async Task Main()
        {
            _totalExecutionStopwatch.Start();
            ExecutionSummary summary = new();
            
            try
            {
                // Get configuration path and load settings
                string configPath = GetConfigPath();
                
                // Load and validate configuration
                AppConfig appConfig = ConfigurationLoader.LoadConfiguration<AppConfig>(configPath);
                DatabaseConfig dbConfig = appConfig.DatabaseConfig;
                ProcessConfig processConfig = appConfig.ProcessConfig;
                LoggingConfig loggingConfig = appConfig.LoggingConfig;
                
                // Initialize logging
                Logger.Initialize(loggingConfig);
                
                Logger.LogInfo($"Using configuration file: {configPath}");
                Logger.LogInfo("Loading configuration...");
                Logger.LogDivider();

                // Print configuration for verification
                PrintConfiguration(dbConfig, processConfig, loggingConfig);
                Logger.LogDivider();

                // Build connection string and test database connection
                string connectionString = BuildConnectionString(dbConfig);
                await TestDatabaseConnection(connectionString);
                Logger.LogDivider();

                // Ensure CSV folder exists
                EnsureCsvFolderExists(processConfig.CsvFolderPath);

                // Process the CSV files
                summary = await ProcessFiles(connectionString, processConfig);

                // Display execution summary
                DisplayExecutionSummary(summary);
                
                Logger.LogSuccess("Process completed successfully! 🎉");
                Logger.LogDivider();
                
                // Auto-exit with timeout instead of waiting for a key press
                AutoExitWithTimeout(processConfig.AutoExitTimeoutSeconds);
            }
            catch (SqlException sqlEx)
            {
                summary.TotalErrors++;
                LogSqlException(sqlEx);
                DisplayExecutionSummary(summary);
                AutoExitWithTimeout(10); // Default to 10 seconds on error
            }
            catch (Exception ex)
            {
                summary.TotalErrors++;
                LogGeneralException(ex);
                DisplayExecutionSummary(summary);
                AutoExitWithTimeout(10); // Default to 10 seconds on error
            }
            finally
            {
                _totalExecutionStopwatch.Stop();
                summary.TotalProcessingTime = _totalExecutionStopwatch.Elapsed;
            }
        }
        
        private static void DisplayExecutionSummary(ExecutionSummary summary)
        {
            Logger.LogDivider();
            Logger.LogInfo("📊 EXECUTION SUMMARY 📊");
            Logger.LogDivider();
            
            Logger.LogInfo($"Total Files Processed: {summary.TotalFilesProcessed}");
            Logger.LogInfo($"Total Rows Processed: {summary.TotalRowsProcessed:N0}");
            Logger.LogInfo($"Total Rows Transferred: {summary.TotalRowsTransferred:N0}");
            
            if (summary.TotalErrors > 0)
            {
                Logger.LogWarning($"Total Errors: {summary.TotalErrors}");
            }
            
            if (summary.TotalWarnings > 0)
            {
                Logger.LogWarning($"Total Warnings: {summary.TotalWarnings}");
            }
            
            TimeSpan totalTime = summary.TotalProcessingTime;
            Logger.LogInfo($"Total Execution Time: {totalTime.Hours:00}:{totalTime.Minutes:00}:{totalTime.Seconds:00}.{totalTime.Milliseconds:000}");
            
            double rowsPerSecond = summary.TotalRowsProcessed / totalTime.TotalSeconds;
            Logger.LogInfo($"Average Performance: {rowsPerSecond:N0} rows/second");
            
            if (summary.FileStats.Count > 0)
            {
                Logger.LogDivider();
                Logger.LogInfo("📄 File Processing Details:");
                
                foreach (var fileStat in summary.FileStats.Values)
                {
                    // Use the WasSuccessful flag to determine the status icon
                    string status = fileStat.WasSuccessful ? "✅" : "❌";
                    Logger.LogInfo($"{status} {fileStat.FileName}:");
                    Logger.LogInfo($"   Rows: {fileStat.RowsProcessed:N0}, Transferred: {fileStat.RowsTransferred:N0}, Columns: {fileStat.ColumnCount}");
                    Logger.LogInfo($"   Processing Time: {fileStat.ProcessingTime.TotalSeconds:N2} seconds");
                    
                    double fileRowsPerSecond = fileStat.ProcessingTime.TotalSeconds > 0 
                        ? fileStat.RowsProcessed / fileStat.ProcessingTime.TotalSeconds 
                        : 0;
                    Logger.LogInfo($"   Performance: {fileRowsPerSecond:N0} rows/second");
                }
            }
            
            Logger.LogDivider();
        }

        private static void AutoExitWithTimeout(int timeoutSeconds)
        {
            Logger.LogInfo($"Application will exit in {timeoutSeconds} seconds...");
            
            // Display a countdown timer
            for (int i = timeoutSeconds; i > 0; i--)
            {
                Console.Write($"\rExiting in {i} seconds... ");
                Thread.Sleep(1000);
            }
            Console.WriteLine("\rExiting now...                ");
        }

        private static void LogSqlException(SqlException sqlEx)
        {
            Logger.LogDivider();
            Logger.LogError("SQL Server Error:", sqlEx);
            Logger.LogInfo($"Error Number: {sqlEx.Number}");
            Logger.LogInfo($"Error State: {sqlEx.State}");
            Logger.LogInfo($"Server: {sqlEx.Server}");
        }

        private static void LogGeneralException(Exception ex)
        {
            Logger.LogDivider();
            Logger.LogError("General Error:", ex);
        }

        private static string GetConfigPath()
        {
            string exePath = AppDomain.CurrentDomain.BaseDirectory;
            string rootConfigPath = Path.Combine(exePath, "config.json");
            string configFolderPath = Path.Combine(exePath, "Configuration", "config.json");

            return File.Exists(rootConfigPath) ? rootConfigPath : File.Exists(configFolderPath) ? configFolderPath : throw new FileNotFoundException("Configuration file not found. Please ensure config.json exists in the application directory or Configuration folder.\n" + $"Tried paths:\n1. {rootConfigPath}\n2. {configFolderPath}");
        }

        private static void PrintConfiguration(DatabaseConfig dbConfig, ProcessConfig processConfig, LoggingConfig loggingConfig)
        {
            Logger.LogConfig("Configuration:");
            Logger.LogConfig($"Server: {dbConfig.Server}");
            Logger.LogConfig($"Database: {dbConfig.Database}");
            Logger.LogConfig($"Windows Auth: {dbConfig.IntegratedSecurity}");
            Logger.LogConfig($"CSV Folder: {processConfig.CsvFolderPath}");
            Logger.LogConfig($"Preserve Leading Zeros: {processConfig.PreserveLeadingZeros}");
            Logger.LogConfig($"Auto Exit Timeout: {processConfig.AutoExitTimeoutSeconds} seconds");
            Logger.LogConfig($"File Logging Enabled: {loggingConfig.EnableFileLogging}");
            if (loggingConfig.EnableFileLogging)
            {
                Logger.LogConfig($"Log Folder: {loggingConfig.LogFolderPath}");
            }
        }

        private static string BuildConnectionString(DatabaseConfig config)
        {
            SqlConnectionStringBuilder builder = new() { 
                DataSource = config.Server, 
                InitialCatalog = config.Database, 
                IntegratedSecurity = config.IntegratedSecurity, 
                TrustServerCertificate = true, 
                // Performance optimization settings
                ConnectTimeout = 30, 
                MultipleActiveResultSets = true, 
                MaxPoolSize = 200,                 // Increased from 100
                MinPoolSize = 5,                   // Preallocation of connections
                ApplicationIntent = ApplicationIntent.ReadWrite,
                Pooling = true,
                LoadBalanceTimeout = 30
            };

            if (!config.IntegratedSecurity && !string.IsNullOrEmpty(config.Username))
            {
                builder.UserID = config.Username;
                builder.Password = config.Password;
            }

            return builder.ConnectionString;
        }

        private static async Task TestDatabaseConnection(string connectionString)
        {
            Logger.LogInfo("Testing database connection...");
            await using SqlConnection connection = new(connectionString);

            try
            {
                await connection.OpenAsync();
                Logger.LogSuccess("Database connection successful!");

                await using SqlCommand command = new("SELECT @@VERSION", connection);
                object? version = await command.ExecuteScalarAsync();
                Logger.LogInfo($"SQL Server Version: {version}");
            }
            catch (SqlException)
            {
                Logger.LogError("Failed to connect to database!");
                throw;
            }
        }

        private static void EnsureCsvFolderExists(string folderPath)
        {
            if (!Directory.Exists(folderPath))
            {
                _ = Directory.CreateDirectory(folderPath);
                Logger.LogSuccess($"Created CSV folder: {folderPath}");
            }
            else
            {
                Logger.LogInfo($"CSV folder exists: {folderPath}");
            }
        }

        private static async Task<ExecutionSummary> ProcessFiles(string connectionString, ProcessConfig config)
        {
            ExecutionSummary summary = new();
            Stopwatch totalStopwatch = new();
            totalStopwatch.Start();
            
            if (!Directory.Exists(config.CsvFolderPath))
            {
                throw new DirectoryNotFoundException($"CSV folder not found: {config.CsvFolderPath}");
            }

            string[] csvFiles = Directory.EnumerateFiles(config.CsvFolderPath, "*.csv").OrderBy(f => f).ToArray();

            Logger.LogDivider();
            Logger.LogInfo($"Found {csvFiles.Length} CSV files to process.");

            if (csvFiles.Length == 0)
            {
                throw new Exception("No CSV files found in the specified folder.");
            }

            await using SqlConnection connection = new(connectionString);
            await connection.OpenAsync();

            DatabaseOperations.CreateLogTables(connection, config.ErrorTableName, config.SuccessLogTableName);

            DatabaseOperations.ValidationConfig validationConfig = new() { 
                StrictColumnValidation = config.ValidateColumnMapping, 
                ContinueOnError = true 
            };

            try
            {
                for (int fileIndex = 0; fileIndex < csvFiles.Length; fileIndex++)
                {
                    string csvFile = csvFiles[fileIndex];
                    bool isFirstFile = fileIndex == 0;
                    string fileName = Path.GetFileName(csvFile);

                    Logger.LogDivider();
                    Logger.LogInfo($"Processing file {fileIndex + 1} of {csvFiles.Length}: {fileName}");

                    FileProcessingStats fileStats = new() { FileName = fileName };
                    Stopwatch fileStopwatch = new();
                    fileStopwatch.Start();
                    
                    try
                    {
                        // Process the CSV file - this loads the data into the temp table
                        fileStats = await LoadAndProcessCsvFile(connection, csvFile, config, isFirstFile, validationConfig);
                        
                        // If we get here, the file was processed successfully
                        fileStats.WasSuccessful = true;
                        summary.TotalFilesProcessed++;
                        summary.TotalRowsProcessed += fileStats.RowsProcessed;
                        summary.TotalRowsTransferred += fileStats.RowsTransferred;
                    }
                    catch (DatabaseOperations.ColumnValidationException cvEx)
                    {
                        Logger.LogWarning($"Skipping file due to validation error: {cvEx.Message}");
                        fileStats.WasSuccessful = false;
                        fileStats.RowsTransferred = 0; // Set transferred rows to 0 when validation fails
                        summary.TotalFilesProcessed++; // Still count this as a processed file
                        summary.TotalRowsProcessed += fileStats.RowsProcessed; // Count rows that were processed
                        // Don't add to TotalRowsTransferred since no rows were actually transferred
                        summary.TotalWarnings++;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError($"Error processing file: {ex.Message}", ex);
                        fileStats.WasSuccessful = false;
                        fileStats.RowsTransferred = 0; // Set transferred rows to 0 when there's an error
                        summary.TotalFilesProcessed++; // Still count this as a processed file
                        summary.TotalRowsProcessed += fileStats.RowsProcessed; // Count rows that were processed
                        // Don't add to TotalRowsTransferred since no rows were actually transferred
                        summary.TotalErrors++;
                    }
                    finally
                    {
                        fileStopwatch.Stop();
                        fileStats.ProcessingTime = fileStopwatch.Elapsed;
                        summary.FileStats[fileName] = fileStats;
                    }
                }
            }
            finally
            {
                // Drop temporary table once all files are processed, regardless of success or failure
                try
                {
                    Logger.LogInfo("Cleaning up temporary tables...");
                    DatabaseOperations.DropTempTable(connection, config.TempTableName);
                }
                catch (Exception ex)
                {
                    Logger.LogWarning($"Failed to drop temporary table: {ex.Message}");
                    summary.TotalWarnings++;
                }
                
                totalStopwatch.Stop();
                summary.TotalProcessingTime = totalStopwatch.Elapsed;
            }
            
            return summary;
        }

        private static async Task<FileProcessingStats> LoadAndProcessCsvFile(SqlConnection connection, string csvFile, ProcessConfig config, bool isFirstFile, DatabaseOperations.ValidationConfig validationConfig)
        {
            Stopwatch stopwatch = new();
            stopwatch.Start();
            string fileName = Path.GetFileName(csvFile);
            FileProcessingStats stats = new() { FileName = fileName };
            
            Logger.LogInfo($"Starting to read and load data from file: {fileName}");

            // Process in batches to reduce memory consumption
            int rowsProcessed = 0;
            foreach (BatchResult dataBatch in CsvProcessor.LoadCSVInBatches(csvFile, connection, config.ErrorTableName, config.BatchSize, config.PreserveLeadingZeros))
            {
                if (dataBatch.IsFirstBatch)
                {
                    stats.ColumnCount = dataBatch.Data.Columns.Count;
                    DatabaseOperations.DropAndCreateTempTable(connection, config.TempTableName, dataBatch.Data);
                    Logger.LogInfo($"Created temporary table with {stats.ColumnCount} columns for file: {fileName}");
                }

                rowsProcessed += dataBatch.Data.Rows.Count;
                stats.RowsProcessed = rowsProcessed;
                double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
                Logger.LogProgress($"Processing... Total rows so far: {stats.RowsProcessed:N0} (Time elapsed: {elapsedSeconds:N2} seconds)");

                await DatabaseOperations.LoadDataIntoTempTableAsync(connection, config.TempTableName, dataBatch.Data);

                if (dataBatch.IsLastBatch)
                {
                    Logger.LogProgress($"Starting data transfer to destination table for file: {fileName}...");

                    try
                    {
                        await DatabaseOperations.TransferToDestinationAsync(connection, config.TempTableName, config.DestinationTableName, config.ErrorTableName, config.SuccessLogTableName, isFirstFile, config.BatchSize, fileName, validationConfig);
                        
                        // Only set transferred rows to processed rows if the transfer was successful
                        stats.RowsTransferred = stats.RowsProcessed;
                    }
                    catch (DatabaseOperations.ColumnValidationException ex)
                    {
                        // If validation fails, no rows are transferred but we still have processed rows
                        stats.RowsTransferred = 0;
                        
                        // Important: Complete the method here without throwing
                        Logger.LogWarning($"Skipping file due to validation error: {ex.Message}");
                        stats.WasSuccessful = false;
                        return stats;
                    }
                    catch (Exception ex)
                    {
                        // If any other error occurs, no rows are transferred but we still have processed rows
                        stats.RowsTransferred = 0;
                        Logger.LogError($"Error transferring data: {ex.Message}", ex);
                        stats.WasSuccessful = false;
                        return stats;
                    }
                }

                dataBatch.Data.Dispose();
            }

            stopwatch.Stop();
            
            // Mark as successful if we get here
            stats.WasSuccessful = true;
            
            Logger.LogSuccess($"Completed processing {fileName} - " + 
                         $"Total Rows: {stats.RowsProcessed:N0}, Columns: {stats.ColumnCount}, " + 
                         $"Total Time: {stopwatch.Elapsed.TotalSeconds:N2} seconds");
                         
            return stats;
        }
    }
}