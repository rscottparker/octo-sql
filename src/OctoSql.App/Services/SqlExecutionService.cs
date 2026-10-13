using System.Data;
using Microsoft.Data.SqlClient;
using OctoSql.App.Models;

namespace OctoSql.App.Services;

public sealed class QueryExecutionResult
{
    public bool Succeeded { get; init; }
    public string Message { get; init; } = string.Empty;
    public TimeSpan Duration { get; init; }
    public DataSet? Data { get; init; }
    public int RecordsAffected { get; init; }
    public IReadOnlyList<string> InfoMessages { get; init; } = [];
}

public sealed class SqlExecutionService
{
    public async Task<QueryExecutionResult> ExecuteAsync(
        ConnectionProfile connection,
        string sql,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return new QueryExecutionResult
            {
                Succeeded = false,
                Message = "Nothing to execute."
            };
        }

        var started = DateTime.UtcNow;
        var infoMessages = new List<string>();
        var data = new DataSet();
        var recordsAffected = 0;

        try
        {
            await using var sqlConnection = new SqlConnection(connection.BuildConnectionString());
            sqlConnection.InfoMessage += (_, args) =>
            {
                if (!string.IsNullOrWhiteSpace(args.Message))
                {
                    infoMessages.Add(args.Message);
                }
            };

            await sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

            await using var command = sqlConnection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = Math.Max(0, connection.CommandTimeoutSeconds);

            await using var reader = await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

            var tableIndex = 0;
            do
            {
                var table = new DataTable(tableIndex == 0 ? "Results" : $"Results {tableIndex + 1}");
                table.Load(reader);
                recordsAffected += table.Rows.Count;
                if (table.Columns.Count > 0)
                {
                    data.Tables.Add(table);
                    tableIndex++;
                }
            }
            while (!reader.IsClosed && await NextResultSafeAsync(reader, cancellationToken).ConfigureAwait(false));

            var duration = DateTime.UtcNow - started;
            var resultSummary = data.Tables.Count == 0
                ? "Command completed."
                : data.Tables.Count == 1
                    ? $"Returned {data.Tables[0].Rows.Count:N0} row(s)."
                    : $"Returned {data.Tables.Count} result set(s).";

            return new QueryExecutionResult
            {
                Succeeded = true,
                Message = resultSummary,
                Duration = duration,
                Data = data,
                RecordsAffected = recordsAffected,
                InfoMessages = infoMessages
            };
        }
        catch (Exception ex)
        {
            return new QueryExecutionResult
            {
                Succeeded = false,
                Message = ex.Message,
                Duration = DateTime.UtcNow - started,
                Data = data.Tables.Count > 0 ? data : null,
                InfoMessages = infoMessages
            };
        }
    }

    public async Task<(bool Ok, string Message)> TestConnectionAsync(
        ConnectionProfile connection,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var sqlConnection = new SqlConnection(connection.BuildConnectionString());
            await sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = sqlConnection.CreateCommand();
            command.CommandText = "SELECT 1";
            command.CommandTimeout = 10;
            _ = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return (true, $"Connected to {connection.Server}" +
                           (string.IsNullOrWhiteSpace(connection.Database) ? string.Empty : $" / {connection.Database}"));
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    private static async Task<bool> NextResultSafeAsync(SqlDataReader reader, CancellationToken cancellationToken)
    {
        try
        {
            return await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }
}
