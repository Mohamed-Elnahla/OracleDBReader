using Microsoft.VisualStudio.TestTools.UnitTesting;
using OracleDBReader;
using System;
using System.Threading.Tasks;
using Moq; // Ensure Moq is referenced in the .csproj
using System.Data;
using System.Collections.Generic;
using System.Text.Json;
using System.Linq;
using System.Threading;

namespace OracleDBReader.Tests
{
    [TestClass] // This attribute is from Microsoft.VisualStudio.TestTools.UnitTesting
    public class OracleDBReaderTests
    {
        private const string TestDataSource = "testDataSource";
        private const string TestUser = "testUser";
        private const string TestPassword = "testPassword";

        [TestInitialize] // This attribute is from Microsoft.VisualStudio.TestTools.UnitTesting
        public void TestInitialize()
        {
            OracleDBReader.DbConnectionFactory = cs => new Mock<IDbConnection>().Object;
        }

        [TestMethod]
        public async Task QueryToJsonAsync_ShouldThrowException_ForNonQuery()
        {
            var sqlQuery = "INSERT INTO Users (Name) VALUES ('Test');";
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery)
            );
        }

        [TestMethod]
        public async Task StreamQueryAsJsonAsync_ShouldThrowException_ForNonQuery()
        {
            var sqlQuery = "UPDATE Users SET Name = 'Test' WHERE Id = 1;";
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in OracleDBReader.StreamQueryAsJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery))
                { /* Loop should not execute */ }
            });
        }

        [TestMethod]
        public async Task StreamQueryParallelAsync_ShouldThrowException_ForNonQuery()
        {
            var sqlQuery = "DELETE FROM Users WHERE Id = 1;";
            Func<Dictionary<string, object?>, Task> rowProcessor = (row) => Task.CompletedTask;
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => OracleDBReader.StreamQueryParallelAsync(TestDataSource, TestUser, TestPassword, sqlQuery, rowProcessor)
            );
        }

        [TestMethod]
        public async Task QueryToJsonAsync_ReturnsCorrectJson_ForSelectQuery_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            // Setup reader for two rows
            var readSequence = new Queue<bool>(new[] { true, true, false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(2);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");
            mockReader.Setup(r => r.GetName(1)).Returns("NAME");
            // Row 1
            mockReader.SetupSequence(r => r.IsDBNull(0)).Returns(false).Returns(false);
            mockReader.SetupSequence(r => r.GetValue(0)).Returns(1).Returns(2);
            mockReader.SetupSequence(r => r.IsDBNull(1)).Returns(false).Returns(true);
            mockReader.SetupSequence(r => r.GetValue(1)).Returns("Test User 1").Returns(DBNull.Value);

            var jsonResult = await OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery);

            var expectedObject = new
            {
                Table = new List<object>
                {
                    new { ID = 1, NAME = "Test User 1" },
                    new { ID = 2, NAME = (object?)null }
                }
            };
            var expectedJson = JsonSerializer.Serialize(expectedObject);
            Assert.AreEqual(expectedJson, jsonResult);
        }

        [TestMethod]
        public async Task StreamQueryAsJsonAsync_StreamsCorrectJson_ForSelectQuery_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            var readSequence = new Queue<bool>(new[] { true, false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(2);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");
            mockReader.Setup(r => r.GetName(1)).Returns("NAME");
            mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
            mockReader.Setup(r => r.GetValue(0)).Returns(10);
            mockReader.Setup(r => r.IsDBNull(1)).Returns(false);
            mockReader.Setup(r => r.GetValue(1)).Returns("Stream User");

            var results = new List<string>();
            await foreach (var jsonRow in OracleDBReader.StreamQueryAsJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery))
            {
                results.Add(jsonRow);
            }

            Assert.IsTrue(results.Count > 0, "Should stream at least one row.");
            var expectedObject = new { Table = new[] { new { ID = 10, NAME = "Stream User" } } };
            var expectedJson = JsonSerializer.Serialize(expectedObject);
            Assert.AreEqual(expectedJson, results[0]);
        }

        [TestMethod]
        public async Task StreamQueryParallelAsync_ProcessesRows_ForSelectQuery_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID FROM Users";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            var readSequence = new Queue<bool>(new[] { true, true, true, false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(1);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");
            mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
            mockReader.SetupSequence(r => r.GetValue(0)).Returns(100).Returns(101).Returns(102);

            var processedIds = new List<int>();
            Func<Dictionary<string, object?>, Task> rowProcessor = async (row) =>
            {
                await Task.Delay(10);
                lock (processedIds)
                {
                    processedIds.Add((int)row["ID"]!);
                }
            };

            await OracleDBReader.StreamQueryParallelAsync(TestDataSource, TestUser, TestPassword, sqlQuery, rowProcessor, maxDegreeOfParallelism: 2);

            Assert.AreEqual(3, processedIds.Count, "Should process all three rows.");
            CollectionAssert.AreEquivalent(new List<int> { 100, 101, 102 }, processedIds, "Processed IDs do not match expected IDs.");
        }

        [TestMethod]
        public async Task QueryToJsonAsync_Handles_EmptyResult_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users WHERE 1=0";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            var readSequence = new Queue<bool>(new[] { false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(2);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");
            mockReader.Setup(r => r.GetName(1)).Returns("NAME");

            var jsonResult = await OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery);

            var expectedObject = new { Table = new object[] { } };
            var expectedJson = JsonSerializer.Serialize(expectedObject);
            Assert.AreEqual(expectedJson, jsonResult);
        }

        [TestMethod]
        public async Task StreamQueryAsJsonAsync_Handles_EmptyResult_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users WHERE 1=0";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            var readSequence = new Queue<bool>(new[] { false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(2);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");
            mockReader.Setup(r => r.GetName(1)).Returns("NAME");

            var results = new List<string>();
            await foreach (var jsonRow in OracleDBReader.StreamQueryAsJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery))
            {
                results.Add(jsonRow);
            }

            Assert.IsFalse(results.Any(), "Should not stream any rows for an empty result.");
        }

        [TestMethod]
        public async Task StreamQueryParallelAsync_Handles_EmptyResult_WithInterfaces()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqlQuery = "SELECT ID FROM Users WHERE 1=0";

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            var readSequence = new Queue<bool>(new[] { false });
            mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
            mockReader.SetupGet(r => r.FieldCount).Returns(1);
            mockReader.Setup(r => r.GetName(0)).Returns("ID");

            var processedCount = 0;
            Func<Dictionary<string, object?>, Task> rowProcessor = (row) =>
            {
                Interlocked.Increment(ref processedCount);
                return Task.CompletedTask;
            };

            await OracleDBReader.StreamQueryParallelAsync(TestDataSource, TestUser, TestPassword, sqlQuery, rowProcessor);

            Assert.AreEqual(0, processedCount, "Should not process any rows for an empty result.");
        }

        [TestMethod]
        public async Task QueryToJsonAsync_Allows_Parallel_And_Cache_Tags()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockReader = new Mock<IDataReader>();
            var sqls = new[]
            {
                "SELECT /*+ PARALLEL */ * FROM DUAL",
                "SELECT /*+ RESULT_CACHE */ * FROM DUAL",
                "WITH /*+ PARALLEL */ t AS (SELECT 1 FROM DUAL) SELECT * FROM t"
            };

            OracleDBReader.DbConnectionFactory = cs => mockConnection.Object;
            mockConnection.Setup(c => c.Open());
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.SetupProperty(c => c.CommandText);
            mockCommand.Setup(c => c.ExecuteReader(CommandBehavior.SequentialAccess)).Returns(mockReader.Object);

            foreach (var sqlQuery in sqls)
            {
                // Reset the read sequence for each query
                var readSequence = new Queue<bool>(new[] { false });
                mockReader.Setup(r => r.Read()).Returns(() => readSequence.Dequeue());
                var jsonResult = await OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery);
                var expectedObject = new { Table = new object[] { } };
                var expectedJson = JsonSerializer.Serialize(expectedObject);
                Assert.AreEqual(expectedJson, jsonResult, $"Failed for query: {sqlQuery}");
            }
        }

        [TestMethod]
        public async Task QueryToJsonAsync_Rejects_NonSelect_With_Parallel_Tag()
        {
            var sqlQuery = "UPDATE /*+ PARALLEL */ Users SET Name = 'X' WHERE 1=0";
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery)
            );
        }
    }
}
