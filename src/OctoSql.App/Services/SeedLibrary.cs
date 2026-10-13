using OctoSql.App.Models;

namespace OctoSql.App.Services;

public static class SeedLibrary
{
    public static QueryLibrary CreateDefault()
    {
        var library = new QueryLibrary
        {
            Connections =
            [
                new ConnectionProfile
                {
                    Name = "Local SQL Server",
                    Server = "localhost",
                    UseWindowsAuth = true,
                    TrustServerCertificate = true
                }
            ]
        };

        library.LastConnectionId = library.Connections[0].Id;

        var diagnostics = new QueryFolder { Name = "Diagnostics" };
        var operations = new QueryFolder { Name = "Operations" };
        var validation = new QueryFolder { Name = "Validation" };

        TryAddQuery(diagnostics, "Data Integrity Diagnostic", "Post-clone AssocID integrity checks", "diagnostic.sql");
        TryAddQuery(operations, "Restore TRN Database", "Restore TRN_CreditDeliveryDB from backup", "restore-script.sql");
        TryAddQuery(operations, "Clone Assoc Data Proc", "Execute usp_Wip15_CloneAssocData", "runner.sql");
        TryAddQuery(operations, "Orchestrator (SQLCMD)", "SQLCMD-mode orchestration of restore/clone/runner", "orchestrator.sql");
        TryAddQuery(operations, "Clone Assoc Data Script", "Full clone association data script", "trn_clone_data.sql");
        TryAddQuery(validation, "Post-Load Validation", "SSIS load and AssocID count checks", "validation.sql");

        library.Root.Folders.Add(diagnostics);
        library.Root.Folders.Add(operations);
        library.Root.Folders.Add(validation);

        if (library.Root.EnumerateQueries().Any() is false)
        {
            library.Root.Queries.Add(new SavedQuery
            {
                Name = "Server Version",
                Description = "Quick connectivity check",
                Sql = "SELECT @@VERSION AS [Version], DB_NAME() AS [Database], SUSER_SNAME() AS [Login];",
                Tags = ["sample", "connectivity"]
            });
        }

        return library;
    }

    private static void TryAddQuery(QueryFolder folder, string name, string description, string fileName)
    {
        var sql = TryReadSeedSql(fileName);
        if (sql is null)
        {
            return;
        }

        folder.Queries.Add(new SavedQuery
        {
            Name = name,
            Description = description,
            Sql = sql,
            Tags = [Path.GetFileNameWithoutExtension(fileName)]
        });
    }

    private static string? TryReadSeedSql(string fileName)
    {
        foreach (var path in GetCandidatePaths(fileName))
        {
            if (File.Exists(path))
            {
                return File.ReadAllText(path);
            }
        }

        return null;
    }

    private static IEnumerable<string> GetCandidatePaths(string fileName)
    {
        var baseDir = AppContext.BaseDirectory;
        yield return Path.Combine(baseDir, "Queries", fileName);
        yield return Path.Combine(baseDir, fileName);

        var current = new DirectoryInfo(baseDir);
        for (var i = 0; i < 6 && current is not null; i++)
        {
            yield return Path.Combine(current.FullName, fileName);
            yield return Path.Combine(current.FullName, "Queries", fileName);
            yield return Path.Combine(current.FullName, "scripts", fileName);
            current = current.Parent;
        }
    }
}
