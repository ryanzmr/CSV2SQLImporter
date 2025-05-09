using Newtonsoft.Json;

namespace CSVDatabaseImporter.Models
{
    public class DatabaseConfig
    {
        public string Server { get; set; } = string.Empty;
        public string Database { get; set; } = string.Empty;
        public bool IntegratedSecurity { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
    }

    public class ProcessConfig
    {
        public string CsvFolderPath { get; set; } = string.Empty;
        public string TempTableName { get; set; } = string.Empty;
        public string DestinationTableName { get; set; } = string.Empty;
        public string ErrorTableName { get; set; } = string.Empty;
        public string SuccessLogTableName { get; set; } = string.Empty;
        public int BatchSize { get; set; }
        public bool ValidateColumnMapping { get; set; } = true;
        public int AutoExitTimeoutSeconds { get; set; } = 10;
        public bool PreserveLeadingZeros { get; set; } = true;
    }

    public class LoggingConfig
    {
        public string LogFolderPath { get; set; } = string.Empty;
        public bool EnableFileLogging { get; set; } = true;
        public string ErrorLogFolder { get; set; } = "Errors";
        public string SuccessLogFolder { get; set; } = "Success";
        public string ConsoleLogFolder { get; set; } = "Console";
    }

    public class AppConfig
    {
        public DatabaseConfig DatabaseConfig { get; set; } = new();
        public ProcessConfig ProcessConfig { get; set; } = new();
        public LoggingConfig LoggingConfig { get; set; } = new();
    }

    public static class ConfigurationLoader
    {
        public static T LoadConfiguration<T>(string configPath) where T : class
        {
            ArgumentNullException.ThrowIfNull(configPath);

            if (!File.Exists(configPath))
            {
                throw new FileNotFoundException($"Configuration file not found: {configPath}");
            }

            string jsonContent = File.ReadAllText(configPath);
            T? result = JsonConvert.DeserializeObject<T>(jsonContent);

            return result ?? throw new InvalidOperationException(
                $"Failed to load configuration from {configPath}");
        }
    }
}