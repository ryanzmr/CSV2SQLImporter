using System.Data;
using System.Globalization;
using System.Text;
using CSVDatabaseImporter.Models;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter.Services
{
    public static class CsvProcessor
    {
        private static readonly string[] DateFormats = ["dd-MM-yyyy", "MM/dd/yyyy", "yyyy-MM-dd"];
        // Cache for parsed dates to avoid repeated parsing of same values
        private static readonly Dictionary<string, string> _dateCache = new(StringComparer.OrdinalIgnoreCase);
        // Cache for numeric conversions to avoid repeated parsing
        private static readonly Dictionary<string, string> _numericCache = new(StringComparer.OrdinalIgnoreCase);

        public static IEnumerable<BatchResult> LoadCSVInBatches(string csvFilePath, SqlConnection connection, string errorTableName, int batchSize, bool preserveLeadingZeros)
        {
            const int bufferSize = 262144;  // Increased to 256KB from 64KB
            using StreamReader sr = new(csvFilePath, Encoding.UTF8, true, bufferSize);
            ArgumentNullException.ThrowIfNull(csvFilePath);
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(errorTableName);
            string? headerLine = sr.ReadLine();

            if (string.IsNullOrEmpty(headerLine))
            {
                throw new ArgumentException("CSV file is empty.");
            }

            string[] headers = ParseCSVLine(headerLine);
            DataTable currentBatch = CreateDataTable(headers);
            bool isFirstBatch = true;

            // Use a StringBuilder to collect lines for batch processing
            StringBuilder batchLines = new(batchSize * 200);  // Estimate 200 chars per line
            int lineCount = 0;

            while (!sr.EndOfStream)
            {
                string? line = sr.ReadLine();
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                batchLines.AppendLine(line);
                lineCount++;

                // Process in mini-batches to balance memory usage and performance
                if (lineCount >= 5000 || sr.EndOfStream)
                {
                    ProcessBatchOfLines(batchLines.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries), 
                                     headers, currentBatch, connection, errorTableName, Path.GetFileName(csvFilePath), preserveLeadingZeros);
                    
                    batchLines.Clear();
                    lineCount = 0;
                }

                if (currentBatch.Rows.Count >= batchSize)
                {
                    yield return new BatchResult { Data = currentBatch, IsFirstBatch = isFirstBatch, IsLastBatch = false };
                    currentBatch = CreateDataTable(headers);
                    isFirstBatch = false;
                }
            }

            if (currentBatch.Rows.Count > 0)
            {
                yield return new BatchResult { Data = currentBatch, IsFirstBatch = isFirstBatch, IsLastBatch = true };
            }
            
            // Clear caches to free memory
            _dateCache.Clear();
            _numericCache.Clear();
        }

        private static void ProcessBatchOfLines(string[] lines, string[] headers, DataTable dt, SqlConnection connection, 
                                             string errorTableName, string fileName, bool preserveLeadingZeros)
        {
            foreach (string line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                try
                {
                    string[] values = ParseCSVLine(line);
                    if (values.Length == headers.Length)
                    {
                        ProcessRow(values, dt, preserveLeadingZeros);
                    }
                    else
                    {
                        LogError(connection, errorTableName, fileName, "Row Structure", "CSV Parsing Error", 
                              $"Row has {values.Length} columns but header has {headers.Length} columns in file {fileName}");
                    }
                }
                catch (Exception ex)
                {
                    LogError(connection, errorTableName, fileName, "Data Processing", "ProcessError", ex.Message);
                }
            }
        }

        private static DataTable CreateDataTable(string[] headers)
        {
            DataTable dt = new();
            foreach (string header in headers)
            {
                _ = dt.Columns.Add(header.Trim());
            }
            return dt;
        }

        private static void ProcessRow(string[] values, DataTable dt, bool preserveLeadingZeros)
        {
            DataRow row = dt.NewRow();
            for (int i = 0; i < values.Length; i++)
            {
                string value = values[i].Trim();

                if (string.IsNullOrWhiteSpace(value))
                {
                    row[i] = DBNull.Value;
                    continue;
                }

                // Improved check for leading zeros
                if (preserveLeadingZeros)
                {
                    // Check for numeric values with leading zeros using regex pattern
                    if (value.StartsWith('0') && System.Text.RegularExpressions.Regex.IsMatch(value, @"^0+\d+$"))
                    {
                        // Store as string to preserve leading zeros
                        row[i] = value;
                        continue;
                    }
                }
                
                // Try to convert to date first using cache
                if (_dateCache.TryGetValue(value, out string? cachedDateValue))
                {
                    row[i] = cachedDateValue;
                }
                // Try to convert to number using cache
                else if (_numericCache.TryGetValue(value, out string? cachedNumericValue))
                {
                    row[i] = cachedNumericValue;
                }
                else
                {
                    // Process value normally, attempting to convert to date or number if appropriate
                    if (DateTime.TryParseExact(value, DateFormats, CultureInfo.InvariantCulture, 
                                          DateTimeStyles.None, out DateTime dateValue))
                    {
                        string formattedDate = dateValue.ToString("yyyy-MM-dd");
                        _dateCache[value] = formattedDate;
                        row[i] = formattedDate;
                    }
                    else if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal numericValue))
                    {
                        // Double-check if the original value had leading zeros
                        if (preserveLeadingZeros && value.Length > 1 && value[0] == '0')
                        {
                            row[i] = value; // Preserve the original format with leading zeros
                        }
                        else
                        {
                            string formattedNumber = numericValue.ToString(CultureInfo.InvariantCulture);
                            _numericCache[value] = formattedNumber;
                            row[i] = formattedNumber;
                        }
                    }
                    else
                    {
                        row[i] = value;
                    }
                }
            }
            dt.Rows.Add(row);
        }

        // Fast CSV line parser - optimized for performance
        private static string[] ParseCSVLine(string line)
        {
            List<string> values = new(50); // Pre-allocate with reasonable capacity
            StringBuilder value = new(100); // Pre-allocate with reasonable capacity
            bool inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == '"')
                {
                    inQuotes = !inQuotes;
                }
                else if (c == ',' && !inQuotes)
                {
                    values.Add(value.ToString());
                    value.Clear();
                }
                else
                {
                    value.Append(c);
                }
            }
            values.Add(value.ToString());
            return values.ToArray();
        }

        private static void LogError(SqlConnection connection, string errorTableName, string fileName, string columnName, string errorType, string reason)
        {
            const string insertErrorQuery =
                @"
                INSERT INTO {0} (FileName, ColumnName, ErrorType, Reason, Timestamp) 
                VALUES (@FileName, @ColumnName, @ErrorType, @Reason, GETDATE())";

            using SqlCommand errorCommand = new(string.Format(insertErrorQuery, errorTableName), connection);

            _ = errorCommand.Parameters.AddWithValue("@FileName", fileName ?? (object)DBNull.Value);
            _ = errorCommand.Parameters.AddWithValue("@ColumnName", columnName ?? (object)DBNull.Value);
            _ = errorCommand.Parameters.AddWithValue("@ErrorType", errorType ?? (object)DBNull.Value);
            _ = errorCommand.Parameters.AddWithValue("@Reason", reason ?? (object)DBNull.Value);
            _ = errorCommand.ExecuteNonQuery();
        }
    }
}