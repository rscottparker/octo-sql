using System.Text.Json;
using System.Text.Json.Serialization;
using OctoSql.App.Models;

namespace OctoSql.App.Services;

public sealed class LibraryStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public string LibraryPath { get; }

    public LibraryStore(string? libraryPath = null)
    {
        var appData = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "OctoSql");
        Directory.CreateDirectory(appData);
        LibraryPath = libraryPath ?? Path.Combine(appData, "library.json");
    }

    public QueryLibrary LoadOrCreate()
    {
        if (File.Exists(LibraryPath))
        {
            try
            {
                var json = File.ReadAllText(LibraryPath);
                var library = JsonSerializer.Deserialize<QueryLibrary>(json, JsonOptions);
                if (library is not null)
                {
                    return library;
                }
            }
            catch
            {
                // Fall through to a fresh library if the file is corrupt.
            }
        }

        var seeded = SeedLibrary.CreateDefault();
        Save(seeded);
        return seeded;
    }

    public void Save(QueryLibrary library)
    {
        var directory = Path.GetDirectoryName(LibraryPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(library, JsonOptions);
        File.WriteAllText(LibraryPath, json);
    }
}
