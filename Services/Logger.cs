using System;
using System.IO;
using System.Text;
using CSVDatabaseImporter.Models;

namespace CSVDatabaseImporter.Services
{
    public static class Logger
    {
        private static string _consoleLogPath = string.Empty;
        private static string _errorLogPath = string.Empty;
        private static string _successLogPath = string.Empty;
        private static bool _enableFileLogging = false;
        private static readonly StringBuilder _consoleBuffer = new();

        public static void Initialize(LoggingConfig config)
        {
            // Set console output encoding to UTF-8 to properly display emojis
            Console.OutputEncoding = Encoding.UTF8;
            
            _enableFileLogging = config.EnableFileLogging;
            
            if (_enableFileLogging)
            {
                // Create log directories if they don't exist
                string baseLogPath = config.LogFolderPath;
                if (!Directory.Exists(baseLogPath))
                {
                    Directory.CreateDirectory(baseLogPath);
                }

                string errorFolder = Path.Combine(baseLogPath, config.ErrorLogFolder);
                string successFolder = Path.Combine(baseLogPath, config.SuccessLogFolder);
                string consoleFolder = Path.Combine(baseLogPath, config.ConsoleLogFolder);

                if (!Directory.Exists(errorFolder)) Directory.CreateDirectory(errorFolder);
                if (!Directory.Exists(successFolder)) Directory.CreateDirectory(successFolder);
                if (!Directory.Exists(consoleFolder)) Directory.CreateDirectory(consoleFolder);

                // Create log file paths with timestamps
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                _errorLogPath = Path.Combine(errorFolder, $"error_{timestamp}.log");
                _successLogPath = Path.Combine(successFolder, $"success_{timestamp}.log");
                _consoleLogPath = Path.Combine(consoleFolder, $"console_{timestamp}.log");

                // Create the console log file
                File.WriteAllText(_consoleLogPath, "");
            }
        }

        // Standard info message with cyan color
        public static void LogInfo(string message)
        {
            string formattedMessage = $"[ℹ️ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // Configuration info with magenta color
        public static void LogConfig(string message)
        {
            string formattedMessage = $"[🔧 {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }
        
        // System info with white color
        public static void LogSystem(string message)
        {
            string formattedMessage = $"[🖥️ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // File operation info with DarkCyan color
        public static void LogFileOp(string message)
        {
            string formattedMessage = $"[📄 {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // Database operation info with DarkBlue color
        public static void LogDbOp(string message)
        {
            string formattedMessage = $"[🗃️ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // Success message with green color
        public static void LogSuccess(string message)
        {
            string formattedMessage = $"[✅ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
            
            if (_enableFileLogging)
            {
                AppendToFile(_successLogPath, formattedMessage);
            }
        }

        // Warning message with yellow color
        public static void LogWarning(string message)
        {
            string formattedMessage = $"[⚠️ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // Error message with red color
        public static void LogError(string message, Exception? ex = null)
        {
            string exceptionInfo = ex != null ? $"\nException: {ex.GetType().Name}\nMessage: {ex.Message}\nStack Trace: {ex.StackTrace}" : string.Empty;
            string formattedMessage = $"[❌ {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}{exceptionInfo}";
            
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
            
            if (_enableFileLogging)
            {
                AppendToFile(_errorLogPath, formattedMessage);
            }
        }

        // Log a divider to create visual separation in console output
        public static void LogDivider()
        {
            string divider = "----------------------------------------";
            Console.WriteLine(divider);
            AppendToConsoleLog(divider);
        }

        // Progress message with DarkGreen color
        public static void LogProgress(string message)
        {
            string formattedMessage = $"[🔄 {DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine(formattedMessage);
            Console.ResetColor();
            AppendToConsoleLog(formattedMessage);
        }

        // Append to the console log buffer and file
        private static void AppendToConsoleLog(string message)
        {
            _consoleBuffer.AppendLine(message);
            if (_enableFileLogging)
            {
                AppendToFile(_consoleLogPath, message);
            }
        }

        // Append a message to a specified file
        private static void AppendToFile(string filePath, string message)
        {
            try
            {
                File.AppendAllText(filePath, message + Environment.NewLine);
            }
            catch (Exception ex)
            {
                // If we can't write to the log file, just show it on console
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error writing to log file: {ex.Message}");
                Console.ResetColor();
            }
        }
    }
}