using System.Data;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter.Services
{
    public static class DatabaseOperations
    {
        private static readonly Stopwatch _stopwatch = new();

        public class ValidationConfig
        {
            public bool StrictColumnValidation { get; set; } = true;
            public bool ContinueOnError { get; set; } = true;
            public int MinimumRequiredColumns { get; set; } = 1;
        }

        public class ColumnMappingValidationResult
        {
            public List<string> SourceColumns { get; set; } = [];
            public List<string> DestinationColumns { get; set; } = [];
            public List<string> MatchedColumns { get; set; } = [];
            public List<string> UnmatchedSourceColumns { get; set; } = [];
            public List<string> UnmatchedDestColumns { get; set; } = [];
            public Dictionary<string, string> DetailedAnalysis { get; set; } = [];
        }

        public class ColumnValidationException : Exception
        {
            public ColumnValidationException() : base() { }
            public ColumnValidationException(string message) : base(message) { }
            public ColumnValidationException(string message, Exception innerException) : base(message, innerException) { }
        }

        public class DataTransferException : Exception
        {
            public DataTransferException() : base() { }
            public DataTransferException(string message) : base(message) { }
            public DataTransferException(string message, Exception innerException) : base(message, innerException) { }
        }

        public static void CreateLogTables(SqlConnection connection, string errorTableName, string successLogTableName)
        {
            string createErrorTableQuery =
                $@"
                IF OBJECT_ID('{errorTableName}', 'U') IS NULL
                CREATE TABLE {errorTableName} (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    FileName NVARCHAR(MAX),
                    ColumnName NVARCHAR(MAX),
                    ErrorType NVARCHAR(100),
                    Reason NVARCHAR(MAX),
                    SourceValue NVARCHAR(MAX),
                    DestinationValue NVARCHAR(MAX),
                    Timestamp DATETIME DEFAULT GETDATE()
                )";

            string createSuccessTableQuery =
                $@"
                IF OBJECT_ID('{successLogTableName}', 'U') IS NULL
                CREATE TABLE {successLogTableName} (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    Message NVARCHAR(MAX),
                    TotalRows BIGINT,
                    SourceColumns INT,
                    DestinationColumns INT,
                    MatchedColumns INT,
                    ProcessingTimeSeconds DECIMAL(18,2),
                    RowsPerSecond INT,
                    Timestamp DATETIME DEFAULT GETDATE()
                )";

            using SqlCommand cmdError = new(createErrorTableQuery, connection);
            _ = cmdError.ExecuteNonQuery();

            using SqlCommand cmdSuccess = new(createSuccessTableQuery, connection);
            _ = cmdSuccess.ExecuteNonQuery();
        }

        public static void DropAndCreateTempTable(SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            StringBuilder sql = new();
            _ = sql.AppendLine("SET NOCOUNT ON;");
            _ = sql.AppendLine($"IF OBJECT_ID('{tempTableName}', 'U') IS NOT NULL DROP TABLE {tempTableName};");
            _ = sql.AppendLine($"CREATE TABLE {tempTableName} (");

            foreach (DataColumn column in dataTable.Columns)
            {
                // Use VARCHAR(4000) instead of NVARCHAR(MAX) for better performance when appropriate
                _ = sql.AppendLine($"[{column.ColumnName}] NVARCHAR(4000),");
            }
            
            // Remove the last comma
            sql.Length -= 3;
            _ = sql.AppendLine();
            _ = sql.AppendLine(") WITH (DATA_COMPRESSION = ROW);"); // Apply row compression for better performance

            // Create a clustered index on the first column for better bulk insert performance
            string firstColumn = dataTable.Columns[0].ColumnName;
            _ = sql.AppendLine($"CREATE CLUSTERED INDEX IX_{tempTableName}_{firstColumn} ON {tempTableName}([{firstColumn}]);");

            using SqlCommand cmd = new(sql.ToString(), connection)
            {
                CommandTimeout = 300 // Increase timeout to 5 minutes for large tables
            };
            _ = cmd.ExecuteNonQuery();
            
            // Set the recovery model to simple for better performance during bulk operations
            sql.Clear();
            _ = sql.AppendLine("SET NOCOUNT ON;");
            _ = sql.AppendLine($"ALTER DATABASE [{connection.Database}] SET RECOVERY SIMPLE;");
            
            using SqlCommand recoveryCmd = new(sql.ToString(), connection)
            {
                CommandTimeout = 60
            };
            try
            {
                _ = recoveryCmd.ExecuteNonQuery();
            }
            catch
            {
                // If we don't have permissions to alter the database, just continue
            }
        }

        public static async Task LoadDataIntoTempTableAsync(SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, null) { DestinationTableName = tempTableName, BatchSize = 50000, BulkCopyTimeout = 0, EnableStreaming = true, NotifyAfter = 50000 };

            foreach (DataColumn column in dataTable.Columns)
            {
                _ = bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
        }

        private static async Task<long> GetTableCountAsync(SqlConnection connection, string tableName, SqlTransaction? transaction)
        {
            await using SqlCommand cmd = new($"SELECT COUNT_BIG(*) FROM {tableName} WITH (NOLOCK)", connection, transaction);

            return Convert.ToInt64(await cmd.ExecuteScalarAsync());
        }

        private static string BuildColumnMismatchMessage(List<string> unmatchedSourceColumns, List<string> unmatchedDestColumns)
        {
            StringBuilder message = new();
            if (unmatchedSourceColumns.Count > 0)
            {
                _ = message.AppendLine("Source columns not found in destination table:");
                foreach (string col in unmatchedSourceColumns)
                {
                    _ = message.AppendLine($" - {col}");
                }
            }
            return message.ToString();
        }

        private static async Task LogErrorAsync(SqlConnection connection, string errorTableName, string fileName, string columnName, string errorType, string reason)
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            const string insertErrorQuery =
                @"
                INSERT INTO {0} 
                    (FileName, ColumnName, ErrorType, Reason, Timestamp) 
                VALUES 
                    (@FileName, @ColumnName, @ErrorType, @Reason, GETDATE())";

            await using SqlCommand cmd = new(string.Format(insertErrorQuery, errorTableName), connection);

            _ = cmd.Parameters.AddWithValue("@FileName", fileName ?? (object)DBNull.Value);
            _ = cmd.Parameters.AddWithValue("@ColumnName", columnName ?? (object)DBNull.Value);
            _ = cmd.Parameters.AddWithValue("@ErrorType", errorType ?? (object)DBNull.Value);
            _ = cmd.Parameters.AddWithValue("@Reason", reason ?? (object)DBNull.Value);

            _ = await cmd.ExecuteNonQueryAsync();
        }
        private static async Task<ColumnMappingValidationResult> ValidateColumnMappingAsync(SqlConnection connection, string sourceTable, string destTable, SqlTransaction? transaction)
        {
            const string columnQuery =
                @"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName
                ORDER BY ORDINAL_POSITION";

            async Task<Dictionary<string, string>> GetColumnDetails(string tableName)
            {
                Dictionary<string, string> columns = [];
                await using SqlCommand cmd = new(columnQuery, connection, transaction);
                _ = cmd.Parameters.AddWithValue("@TableName", tableName);

                await using SqlDataReader reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    string columnName = reader.GetString(0);
                    string dataType = BuildColumnTypeDescription(reader);
                    columns[columnName] = dataType;
                }
                return columns;
            }

            Dictionary<string, string> sourceColumns = await GetColumnDetails(sourceTable);
            Dictionary<string, string> destColumns = await GetColumnDetails(destTable);
            ColumnMappingValidationResult result = new();

            foreach (KeyValuePair<string, string> sourceCol in sourceColumns)
            {
                if (destColumns.TryGetValue(sourceCol.Key, out string? destType))
                {
                    result.MatchedColumns.Add(sourceCol.Key);

                    if (!AreCompatibleTypes(sourceCol.Value, destType))
                    {
                        result.DetailedAnalysis[sourceCol.Key] = $"Data type mismatch: Source={sourceCol.Value}, Destination={destType}";
                    }
                }
                else
                {
                    result.UnmatchedSourceColumns.Add(sourceCol.Key);
                }
            }

            result.UnmatchedDestColumns = destColumns.Keys.Except(result.MatchedColumns).ToList();

            return result;
        }

        private static async Task BulkCopyDataAsync(SqlConnection connection, string sourceTable, string destinationTable, List<string> columnMapping, int batchSize, SqlTransaction? transaction)
        {
            string selectQuery = $"SELECT {string.Join(", ", columnMapping.Select(c => $"[{c}]"))} FROM {sourceTable} WITH (TABLOCK)";

            await using SqlCommand selectCommand = new(selectQuery, connection, transaction);
            await using SqlDataReader reader = await selectCommand.ExecuteReaderAsync();

            using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, transaction) 
            { 
                DestinationTableName = destinationTable, 
                BatchSize = batchSize, 
                BulkCopyTimeout = 0, 
                EnableStreaming = true, 
                NotifyAfter = batchSize 
            };

            // Increase internal buffer size for better performance
            typeof(SqlBulkCopy).GetProperty("BatchSize", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)?.SetValue(bulkCopy, Math.Min(batchSize, 100000));
            typeof(SqlBulkCopy).GetProperty("BulkCopyTimeout", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)?.SetValue(bulkCopy, 0);

            foreach (string column in columnMapping)
            {
                _ = bulkCopy.ColumnMappings.Add(column, column);
            }

            bulkCopy.SqlRowsCopied += (sender, e) => {
                double elapsedSeconds = _stopwatch.Elapsed.TotalSeconds;
                Logger.LogProgress($"Processed {e.RowsCopied:N0} total rows... (Time elapsed: {elapsedSeconds:N2} seconds)");
            };

            await bulkCopy.WriteToServerAsync(reader);
        }

        private static string BuildColumnTypeDescription(SqlDataReader reader)
        {
            string dataType = reader.GetString(1);
            object? maxLength = reader.IsDBNull(2) ? null : reader.GetValue(2);
            object? precision = reader.IsDBNull(3) ? null : reader.GetValue(3);
            object? scale = reader.IsDBNull(4) ? null : reader.GetValue(4);

            return dataType.ToLower() switch
            {
                "nvarchar" or "varchar" or "char" or "nchar" when maxLength != null => $"{dataType}({(Convert.ToInt32(maxLength) == -1 ? "MAX" : maxLength.ToString())})",
                "decimal" or "numeric" when precision != null && scale != null => $"{dataType}({precision},{scale})",
                _ => dataType
            };
        }

        private static bool AreCompatibleTypes(string sourceType, string destType)
        {
            if (sourceType == destType)
            {
                return true;
            }

            Dictionary<string, HashSet<string>> compatibleTypes = new() { ["nvarchar"] = ["varchar", "nvarchar", "nchar", "char", "text", "ntext"], ["varchar"] = ["nvarchar", "varchar", "nchar", "char", "text"], ["int"] = ["bigint", "decimal", "numeric"], ["decimal"] = ["numeric", "float", "real"], ["datetime"] = ["datetime2", "smalldatetime", "date"] };

            static string GetBaseType(string fullType)
            {
                return fullType.Split('(')[0].ToLower();
            }

            string sourceBaseType = GetBaseType(sourceType);
            string destBaseType = GetBaseType(destType);

            return compatibleTypes.TryGetValue(sourceBaseType, out HashSet<string>? compatibleSet) &&
                   compatibleSet.Contains(destBaseType);
        }

        private static async Task LogDetailedSuccessAsync(SqlConnection connection, string successLogTableName, string fileName, long rowsTransferred, int matchedColumns, int unmatchedSourceColumns, int unmatchedDestColumns, SqlTransaction? transaction)
        {
            TimeSpan elapsed = _stopwatch.Elapsed;
            double rowsPerSecond = rowsTransferred / elapsed.TotalSeconds;

            string message = $"File: {fileName} - Transferred: {rowsTransferred:N0} rows, Matched columns: {matchedColumns}, Unmatched source: {unmatchedSourceColumns}, Unmatched dest: {unmatchedDestColumns}, Time: {elapsed.TotalSeconds:N2} s, Rate: {rowsPerSecond:N0} rows/sec";

            const string query =
                @"
                INSERT INTO {0} 
                    (Message, TotalRows, SourceColumns, DestinationColumns, MatchedColumns, 
                     ProcessingTimeSeconds, RowsPerSecond, Timestamp)
                VALUES 
                    (@Message, @Rows, @SrcCols, @DestCols, @MatchedCols, 
                     @ProcTime, @RowsPerSec, GETDATE())";

            await using SqlCommand cmd = new(string.Format(query, successLogTableName), connection, transaction);
            _ = cmd.Parameters.AddWithValue("@Message", message);
            _ = cmd.Parameters.AddWithValue("@Rows", rowsTransferred);
            _ = cmd.Parameters.AddWithValue("@SrcCols", matchedColumns + unmatchedSourceColumns);
            _ = cmd.Parameters.AddWithValue("@DestCols", matchedColumns + unmatchedDestColumns);
            _ = cmd.Parameters.AddWithValue("@MatchedCols", matchedColumns);
            _ = cmd.Parameters.AddWithValue("@ProcTime", elapsed.TotalSeconds);
            _ = cmd.Parameters.AddWithValue("@RowsPerSec", (int)rowsPerSecond);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public static async Task TransferToDestinationAsync(SqlConnection connection, string tempTableName, string destinationTableName, string errorTableName, string successLogTableName, bool isFirstFile, int batchSize, string csvFileName, ValidationConfig validationConfig)
        {
            try
            {
                if (isFirstFile)
                {
                    Logger.LogInfo($"Destination table '{destinationTableName}' will be truncated for the first file.");
                }

                // Create a separate connection for error logging
                await using SqlConnection errorConnection = new(connection.ConnectionString);
                await errorConnection.OpenAsync();

                // First validate the columns
                ColumnMappingValidationResult mappingValidation = await ValidateColumnMappingAsync(connection, tempTableName, destinationTableName,
                                                                                                   null);  // No transaction for validation

                if (validationConfig.StrictColumnValidation && mappingValidation.UnmatchedSourceColumns.Count > 0)
                {
                    string mismatchMessage = BuildColumnMismatchMessage(mappingValidation.UnmatchedSourceColumns, mappingValidation.UnmatchedDestColumns);

                    // Log error outside any transaction
                    await LogErrorAsync(errorConnection, errorTableName, csvFileName, "Column Validation", "ColumnMismatchError", mismatchMessage);

                    throw new ColumnValidationException($"Column validation failed for file {csvFileName}. Some CSV columns are missing in destination table.\n{mismatchMessage}");
                }

                // If validation passes, proceed with data transfer
                await using System.Data.Common.DbTransaction dbTransaction = await connection.BeginTransactionAsync();
                SqlTransaction? transaction = dbTransaction as SqlTransaction;

                try
                {
                    await using (SqlCommand cmd = new("SET XACT_ABORT ON;", connection, transaction))
                    {
                        _ = await cmd.ExecuteNonQueryAsync();
                    }

                    if (isFirstFile)
                    {
                        await using SqlCommand truncateCmd = new($"TRUNCATE TABLE {destinationTableName}", connection, transaction);
                        _ = await truncateCmd.ExecuteNonQueryAsync();
                        Logger.LogInfo($"Destination table '{destinationTableName}' truncated for the first file.");
                    }

                    _stopwatch.Restart();

                    long initialCount = await GetTableCountAsync(connection, destinationTableName, transaction);

                    await BulkCopyDataAsync(connection, tempTableName, destinationTableName, mappingValidation.MatchedColumns, batchSize, transaction);

                    long finalCount = await GetTableCountAsync(connection, destinationTableName, transaction);
                    long rowsTransferred = finalCount - initialCount;

                    long sourceCount = await GetTableCountAsync(connection, tempTableName, transaction);

                    if (rowsTransferred != sourceCount)
                    {
                        string message = $"Row count mismatch. Source: {sourceCount}, Transferred: {rowsTransferred}";
                        await LogErrorAsync(errorConnection, errorTableName, csvFileName, "Data Transfer", "RowCountMismatch", message);
                        throw new DataTransferException(message);
                    }

                    await LogDetailedSuccessAsync(connection, successLogTableName, csvFileName, rowsTransferred, mappingValidation.MatchedColumns.Count, mappingValidation.UnmatchedSourceColumns.Count, mappingValidation.UnmatchedDestColumns.Count, transaction);

                    if (transaction != null)
                    {
                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    if (transaction != null)
                    {
                        await transaction.RollbackAsync();
                    }

                    // Log transfer error outside transaction
                    await LogErrorAsync(errorConnection, errorTableName, csvFileName, "Data Transfer", "TransferError", ex.Message);
                    throw;
                }
            }
            catch (ColumnValidationException)
            {
                // We need to re-throw the ColumnValidationException here instead of swallowing it,
                // so that the ProcessSingleFile method can properly handle it
                throw;
            }
            catch (Exception ex)
            {
                // Create a new connection for logging the error
                await using SqlConnection errorConnection = new(connection.ConnectionString);
                await errorConnection.OpenAsync();

                await LogErrorAsync(errorConnection, errorTableName, csvFileName, "Process Error", ex.GetType().Name, ex.Message);

                Logger.LogError($"Error processing file: {ex.Message}");
                throw; // Re-throw the exception to be handled by the caller
            }
        }

        public static void DropTempTable(SqlConnection connection, string tempTableName)
        {
            try
            {
                string sql = $"IF OBJECT_ID('{tempTableName}', 'U') IS NOT NULL DROP TABLE {tempTableName};";
                using SqlCommand cmd = new(sql, connection);
                _ = cmd.ExecuteNonQuery();
                
                Logger.LogDbOp($"Temporary table '{tempTableName}' has been dropped successfully.");
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error dropping temporary table '{tempTableName}': {ex.Message}");
                throw;
            }
        }
    }
}