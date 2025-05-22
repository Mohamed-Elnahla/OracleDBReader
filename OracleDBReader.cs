using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace OracleDBReader
{
    /// <summary>
    /// Provides methods to stream Oracle database query results as JSON.
    /// </summary>
    public class OracleDBReader : IDisposable
    {
        private bool _disposed;

        /// <summary>
        /// Executes a SQL query and returns the result as a JSON string with the table name set to "Table".
        /// </summary>
        /// <param name="dataSource">The Oracle data source.</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="sqlQuery">The SQL query to execute.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A JSON string representing the result set.</returns>
        public async Task<string> QueryToJsonAsync(string dataSource, string username, string password, string sqlQuery, CancellationToken cancellationToken = default)
        {
            var rows = new List<Dictionary<string, object?>>();
            await foreach (var row in StreamQueryRowsAsync(dataSource, username, password, sqlQuery, cancellationToken))
            {
                rows.Add(row);
            }
            var table = new Dictionary<string, object?> { ["Table"] = rows };
            return JsonSerializer.Serialize(table);
        }

        /// <summary>
        /// Streams each row of the query result as a JSON string.
        /// </summary>
        /// <param name="dataSource">The Oracle data source.</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="sqlQuery">The SQL query to execute.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>An async enumerable of JSON strings, each representing a row.</returns>
        public async IAsyncEnumerable<string> StreamQueryAsJsonAsync(string dataSource, string username, string password, string sqlQuery, CancellationToken cancellationToken = default)
        {
            await foreach (var row in StreamQueryRowsAsync(dataSource, username, password, sqlQuery, cancellationToken))
            {
                var table = new Dictionary<string, object?> { ["Table"] = new[] { row } };
                yield return JsonSerializer.Serialize(table);
            }
        }

        /// <summary>
        /// Streams each row of the query result as a dictionary.
        /// </summary>
        /// <param name="dataSource">The Oracle data source.</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="sqlQuery">The SQL query to execute.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>An async enumerable of dictionaries representing rows.</returns>
        private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryRowsAsync(string dataSource, string username, string password, string sqlQuery, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var connString = $"Data Source={dataSource};User Id={username};Password={password};";
            await using var conn = new OracleConnection(connString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sqlQuery;
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
            var fieldCount = reader.FieldCount;
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < fieldCount; i++)
                {
                    row[reader.GetName(i)] = await reader.IsDBNullAsync(i, cancellationToken) ? null : reader.GetValue(i);
                }
                yield return row;
            }
        }

        /// <summary>
        /// Streams and processes rows in parallel using a callback.
        /// </summary>
        /// <param name="dataSource">The Oracle data source.</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="sqlQuery">The SQL query to execute.</param>
        /// <param name="rowProcessor">A callback to process each row in parallel.</param>
        /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        public async Task StreamQueryParallelAsync(string dataSource, string username, string password, string sqlQuery, Func<Dictionary<string, object?>, Task> rowProcessor, int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default)
        {
            var throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();
            await foreach (var row in StreamQueryRowsAsync(dataSource, username, password, sqlQuery, cancellationToken))
            {
                await throttler.WaitAsync(cancellationToken);
                tasks.Add(Task.Run(async () =>
                {
                    try { await rowProcessor(row); }
                    finally { throttler.Release(); }
                }, cancellationToken));
            }
            await Task.WhenAll(tasks);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}
