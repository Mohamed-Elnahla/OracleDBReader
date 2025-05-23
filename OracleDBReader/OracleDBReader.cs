﻿using System;
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
    public static class OracleDBReader // Made class static
    {
        private const string OnlySelectError = "Only SELECT queries are allowed. Non-read actions are not permitted.";

        /// <summary>
        /// Factory for creating <see cref="IDbConnection"/> instances.
        /// </summary>
        /// <remarks>
        /// By default, this factory creates <see cref="OracleConnection"/> instances using the provided connection string.
        /// It can be overridden for testing purposes to provide mock or alternative implementations of <see cref="IDbConnection"/>.
        /// </remarks>
        public static Func<string, IDbConnection> DbConnectionFactory { get; set; } = cs => new OracleConnection(cs);

        private static void EnsureSelectQuery(string sqlQuery)
        {
            var trimmed = sqlQuery.TrimStart();
            // Accept SELECT or WITH, possibly followed by whitespace and Oracle hints (/*+ ... */)
            var selectPattern = @"^(SELECT|WITH)\s*((/\*\+.*?\*/\s*)*)";
            if (System.Text.RegularExpressions.Regex.IsMatch(trimmed, selectPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                return;
            throw new InvalidOperationException(OnlySelectError);
        }

        // Helper to extract column names from a data reader
        private static string[] GetColumnNames(IDataReader reader)
        {
            var columnNames = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                columnNames[i] = reader.GetName(i);
            return columnNames;
        }

        // Internal helper to build a row dictionary
        private static async Task<Dictionary<string, object?>> BuildRowInternal(
            string[] columnNames,
            Func<int, Task<object?>> getValueAsync)
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < columnNames.Length; i++)
            {
                row[columnNames[i]] = await getValueAsync(i);
            }
            return row;
        }

        // Helper to build a row dictionary (sync)
        private static Dictionary<string, object?> BuildRow(IDataReader reader, string[] columnNames)
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < columnNames.Length; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            return row;
        }

        // Helper to build a row dictionary (async, generic IDataReader)
        private static async Task<Dictionary<string, object?>> BuildRowAsync(IDataReader reader, string[] columnNames, CancellationToken cancellationToken)
        {
            return await BuildRowInternal(
                columnNames,
                async i =>
                {
                    if (reader is System.Data.Common.DbDataReader dbAsyncReader)
                    {
                        return await dbAsyncReader.IsDBNullAsync(i, cancellationToken) ? null : dbAsyncReader.GetValue(i);
                    }
                    return reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
            );
        }

        // Helper to open a connection asynchronously if possible, otherwise synchronously
        private static async Task OpenConnectionAsync(IDbConnection conn, CancellationToken cancellationToken)
        {
            if (conn is System.Data.Common.DbConnection dbConn)
            {
                await dbConn.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Note: Synchronous connection open may block async flow. Only used for non-async IDbConnection implementations.
                conn.Open();
            }
        }

        // Shared helper to yield rows from a data reader (async or sync)
        private static async IAsyncEnumerable<Dictionary<string, object?>> ReadRowsAsync(
            IDataReader reader,
            string[] columnNames,
            Func<IDataReader, string[], CancellationToken, Task<Dictionary<string, object?>>> buildRowAsync,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (reader is System.Data.Common.DbDataReader dbAsyncReader)
            {
                while (await dbAsyncReader.ReadAsync(cancellationToken))
                {
                    yield return await buildRowAsync(dbAsyncReader, columnNames, cancellationToken);
                }
            }
            else
            {
                while (reader.Read())
                {
                    if (cancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException(cancellationToken);
                    yield return await buildRowAsync(reader, columnNames, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Executes a SQL query and returns the result as a JSON string with the table name set to "Table".
        /// </summary>
        /// <param name="dataSource">The Oracle data source.</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="sqlQuery">The SQL query to execute.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A JSON string representing the result set.</returns>
        public static async Task<string> QueryToJsonAsync(string dataSource, string username, string password, string sqlQuery, CancellationToken cancellationToken = default)
        {
            EnsureSelectQuery(sqlQuery);

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
        public static async IAsyncEnumerable<string> StreamQueryAsJsonAsync(string dataSource, string username, string password, string sqlQuery, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            EnsureSelectQuery(sqlQuery);

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
        private static async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryRowsAsync(string dataSource, string username, string password, string sqlQuery, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            EnsureSelectQuery(sqlQuery);
            var connString = $"Data Source={dataSource};User Id={username};Password={password};";
            var conn = DbConnectionFactory(connString);
            if (conn is OracleConnection oracleConn)
            {
                await using (oracleConn)
                {
                    await oracleConn.OpenAsync(cancellationToken);
                    await using var cmd = oracleConn.CreateCommand();
                    cmd.CommandText = sqlQuery;
                    await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
                    var columnNames = GetColumnNames(reader);
                    await foreach (var row in ReadRowsAsync(reader, columnNames, async (r, cols, ct) => await BuildRowAsync(r, cols, ct), cancellationToken))
                    {
                        yield return row;
                    }
                }
            }
            else
            {
                using (conn)
                {
                    await OpenConnectionAsync(conn, cancellationToken);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = sqlQuery;
                    using var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
                    var columnNames = GetColumnNames(reader);
                    await foreach (var row in ReadRowsAsync(reader, columnNames, (r, cols, ct) => Task.FromResult(BuildRow(r, cols)), cancellationToken))
                    {
                        yield return row;
                    }
                }
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
        public static async Task StreamQueryParallelAsync(string dataSource, string username, string password, string sqlQuery, Func<Dictionary<string, object?>, Task> rowProcessor, int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default)
        {
            EnsureSelectQuery(sqlQuery);

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
    }
}
