using Microsoft.VisualStudio.TestTools.UnitTesting;
using OracleDBReader;
using System;
using System.Threading.Tasks;
using Moq; // Ensure Moq is referenced in the .csproj
using Oracle.ManagedDataAccess.Client; // Ensure Oracle.ManagedDataAccess.Client is referenced
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
            OracleDBReader.OracleConnectionFactory = cs => new OracleConnection(cs);
        }

        [TestMethod] // This attribute is from Microsoft.VisualStudio.TestTools.UnitTesting
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
        public async Task QueryToJsonAsync_ReturnsCorrectJson_ForSelectQuery()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            int readCallCount = 0;
            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => { readCallCount++; return readCallCount <= 2; });
            mockDataReader.SetupGet(r => r.FieldCount).Returns(2);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");
            mockDataReader.Setup(r => r.GetName(1)).Returns("NAME");

            // Setup for row 1
            mockDataReader.Setup(r => r.IsDBNullAsync(0, It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => readCallCount == 1 ? false : throw new InvalidOperationException("IsDBNullAsync(0) unexpected call"));
            mockDataReader.Setup(r => r.GetValue(0))
                          .Returns(() => readCallCount == 1 ? 1 : throw new InvalidOperationException("GetValue(0) unexpected call"));
            mockDataReader.Setup(r => r.IsDBNullAsync(1, It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => readCallCount == 1 ? false : (readCallCount == 2 ? true : throw new InvalidOperationException("IsDBNullAsync(1) unexpected call")));
            mockDataReader.Setup(r => r.GetValue(1))
                          .Returns(() => readCallCount == 1 ? "Test User 1" : throw new InvalidOperationException("GetValue(1) unexpected call for non-null"));

            // Setup for row 2 (ID and NAME=null)
            mockDataReader.Setup(r => r.IsDBNullAsync(0, It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => readCallCount == 2 ? false : (readCallCount == 1 ? false : throw new InvalidOperationException("IsDBNullAsync(0) unexpected call for row 2 logic")));
            mockDataReader.Setup(r => r.GetValue(0))
                          .Returns(() => readCallCount == 2 ? 2 : (readCallCount == 1 ? 1 : throw new InvalidOperationException("GetValue(0) unexpected call for row 2 logic")));

            var jsonResult = await OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery);

            var expectedObject = new
            {
                Table = new[]
                {
                    new { ID = 1, NAME = "Test User 1" },
                    new { ID = 2, NAME = (string?)null }
                }
            };
            var expectedJson = JsonSerializer.Serialize(expectedObject);
            Assert.AreEqual(expectedJson, jsonResult);
        }

        [TestMethod]
        public async Task StreamQueryAsJsonAsync_StreamsCorrectJson_ForSelectQuery()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            var readCallCount = 0;
            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => ++readCallCount <= 1);
            mockDataReader.SetupGet(r => r.FieldCount).Returns(2);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");
            mockDataReader.Setup(r => r.GetName(1)).Returns("NAME");
            mockDataReader.Setup(r => r.IsDBNullAsync(0, It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.Setup(r => r.GetValue(0)).Returns(10);
            mockDataReader.Setup(r => r.IsDBNullAsync(1, It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.Setup(r => r.GetValue(1)).Returns("Stream User");

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
        public async Task StreamQueryParallelAsync_ProcessesRows_ForSelectQuery()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID FROM Users";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            var readCallCount = 0;
            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
                          .ReturnsAsync(() => ++readCallCount <= 3);
            mockDataReader.SetupGet(r => r.FieldCount).Returns(1);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");
            mockDataReader.Setup(r => r.IsDBNullAsync(0, It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.SetupSequence(r => r.GetValue(0)).Returns(100).Returns(101).Returns(102);

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
        public async Task QueryToJsonAsync_Handles_EmptyResult()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users WHERE 1=0";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.SetupGet(r => r.FieldCount).Returns(2);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");
            mockDataReader.Setup(r => r.GetName(1)).Returns("NAME");

            var jsonResult = await OracleDBReader.QueryToJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery);

            var expectedObject = new { Table = new object[] { } };
            var expectedJson = JsonSerializer.Serialize(expectedObject);
            Assert.AreEqual(expectedJson, jsonResult);
        }

        [TestMethod]
        public async Task StreamQueryAsJsonAsync_Handles_EmptyResult()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID, NAME FROM Users WHERE 1=0";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.SetupGet(r => r.FieldCount).Returns(2);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");
            mockDataReader.Setup(r => r.GetName(1)).Returns("NAME");

            var results = new List<string>();
            await foreach (var jsonRow in OracleDBReader.StreamQueryAsJsonAsync(TestDataSource, TestUser, TestPassword, sqlQuery))
            {
                results.Add(jsonRow);
            }

            Assert.IsFalse(results.Any(), "Should not stream any rows for an empty result.");
        }

        [TestMethod]
        public async Task StreamQueryParallelAsync_Handles_EmptyResult()
        {
            var mockConnection = new Mock<OracleConnection>();
            var mockCommand = new Mock<OracleCommand>();
            var mockDataReader = new Mock<OracleDataReader>();
            var sqlQuery = "SELECT ID FROM Users WHERE 1=0";

            OracleDBReader.OracleConnectionFactory = cs => mockConnection.Object;

            mockConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockConnection.Setup(c => c.CreateCommand()).Returns(mockCommand.Object);
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(mockDataReader.Object);

            mockDataReader.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            mockDataReader.SetupGet(r => r.FieldCount).Returns(1);
            mockDataReader.Setup(r => r.GetName(0)).Returns("ID");

            var processedCount = 0;
            Func<Dictionary<string, object?>, Task> rowProcessor = (row) =>
            {
                Interlocked.Increment(ref processedCount);
                return Task.CompletedTask;
            };

            await OracleDBReader.StreamQueryParallelAsync(TestDataSource, TestUser, TestPassword, sqlQuery, rowProcessor);

            Assert.AreEqual(0, processedCount, "Should not process any rows for an empty result.");
        }
    }
}
