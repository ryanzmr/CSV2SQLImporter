using System.Data;

namespace CSVDatabaseImporter.Models
{
    public class BatchResult
    {
        private readonly DataTable _data = new();

        public DataTable Data
        {
            get => _data;
            init => _data = value ?? throw new ArgumentNullException(nameof(value));
        }

        public bool IsFirstBatch { get; init; }
        public bool IsLastBatch { get; init; }

        // Helper methods for validation
        public int ColumnCount => Data.Columns.Count;
        public int RowCount => Data.Rows.Count;

        public string[] GetColumnNames()
        {
            string[] columnNames = new string[Data.Columns.Count];
            for (int i = 0; i < Data.Columns.Count; i++)
            {
                columnNames[i] = Data.Columns[i].ColumnName;
            }
            return columnNames;
        }
    }
}