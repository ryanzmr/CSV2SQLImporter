namespace CSVDatabaseImporter.Models
{
    public class ExecutionSummary
    {
        public int TotalFilesProcessed { get; set; }
        public long TotalRowsProcessed { get; set; }
        public long TotalRowsTransferred { get; set; }
        public int TotalErrors { get; set; }
        public int TotalWarnings { get; set; }
        public TimeSpan TotalProcessingTime { get; set; }
        public Dictionary<string, FileProcessingStats> FileStats { get; set; } = [];
    }

    public class FileProcessingStats
    {
        public string FileName { get; set; } = string.Empty;
        public long RowsProcessed { get; set; }
        public long RowsTransferred { get; set; }
        public int ColumnCount { get; set; }
        public TimeSpan ProcessingTime { get; set; }
        public bool WasSuccessful { get; set; }
    }
}