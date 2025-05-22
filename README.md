# OracleDBReader

[![NuGet Version](https://img.shields.io/nuget/v/OracleDBReader.svg)](https://www.nuget.org/packages/OracleDBReader)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![.NET 8.0](https://img.shields.io/badge/.NET-8.0-blue.svg)](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)

**OracleDBReader** is a high-performance, easy-to-use .NET 8 library for efficiently streaming large Oracle database result sets as JSON. It supports asynchronous and parallel row processing, making it ideal for ETL, data integration, and analytics scenarios where memory efficiency and scalability are critical.

---

## Features

- **Stream Oracle query results as JSON** without loading all data into memory
- **Async and parallel row processing** for high throughput
- **Optimized for large result sets** and production ETL/data pipelines
- **Simple API** for integration in .NET 8 projects
- **Supports cancellation tokens** for responsive applications
- **Open source** under the Apache 2.0 license

---

## Installation

Install from [NuGet.org](https://www.nuget.org/packages/OracleDBReader):

```sh
dotnet add package OracleDBReader
```

---

## Quick Start

### Query entire result as JSON

```csharp
using var dbReader = new OracleDBReader();
string json = await dbReader.QueryToJsonAsync(dataSource, username, password, sqlQuery);
```

### Stream each row as JSON

```csharp
using var dbReader = new OracleDBReader();
await foreach (var jsonRow in dbReader.StreamQueryAsJsonAsync(dataSource, username, password, sqlQuery))
{
    // Process each JSON row
}
```

### Parallel row processing

```csharp
using var dbReader = new OracleDBReader();
await dbReader.StreamQueryParallelAsync(
    dataSource, username, password, sqlQuery,
    async row =>
    {
        // Process row (Dictionary<string, object?>)
    },
    maxDegreeOfParallelism: 8
);
```

---

## API Reference

### `OracleDBReader` class

#### `Task<string> QueryToJsonAsync(...)`

Executes a query and returns the entire result set as a JSON string. The result is wrapped in a table object with the name `"Table"`.

#### `IAsyncEnumerable<string> StreamQueryAsJsonAsync(...)`

Streams each row as a JSON string (with table name `"Table"`). Suitable for processing or exporting very large result sets row-by-row.

#### `Task StreamQueryParallelAsync(...)`

Streams and processes rows in parallel using the provided callback. Allows you to specify the maximum degree of parallelism for high-throughput ETL scenarios.

---

## Best Practices

- Use `StreamQueryAsJsonAsync` or `StreamQueryParallelAsync` for large result sets to avoid high memory usage.
- Always use `CancellationToken` in production for responsive cancellation.
- Secure your database credentials and avoid hardcoding them in source code.
- Dispose of the `OracleDBReader` instance after use (use `using var ...`).
- For best performance, tune `maxDegreeOfParallelism` based on your workload and system resources.

---

## License

Apache 2.0

---

## Repository

[https://github.com/Mohamed-Elnahla/OracleDBReader](https://github.com/Mohamed-Elnahla/OracleDBReader)

---

## NuGet Package Metadata

This README is included in the NuGet package and provides usage, API reference, and best practices for OracleDBReader. For the latest updates, visit the [GitHub repository](https://github.com/Mohamed-Elnahla/OracleDBReader) or [NuGet.org](https://www.nuget.org/packages/OracleDBReader).
