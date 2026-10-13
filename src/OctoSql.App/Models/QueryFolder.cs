namespace OctoSql.App.Models;

public sealed class QueryFolder
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "New Folder";
    public List<QueryFolder> Folders { get; set; } = [];
    public List<SavedQuery> Queries { get; set; } = [];

    public IEnumerable<SavedQuery> EnumerateQueries()
    {
        foreach (var query in Queries)
        {
            yield return query;
        }

        foreach (var folder in Folders)
        {
            foreach (var query in folder.EnumerateQueries())
            {
                yield return query;
            }
        }
    }

    public QueryFolder? FindFolder(Guid id)
    {
        if (Id == id)
        {
            return this;
        }

        foreach (var folder in Folders)
        {
            var match = folder.FindFolder(id);
            if (match is not null)
            {
                return match;
            }
        }

        return null;
    }

    public SavedQuery? FindQuery(Guid id)
    {
        foreach (var query in Queries)
        {
            if (query.Id == id)
            {
                return query;
            }
        }

        foreach (var folder in Folders)
        {
            var match = folder.FindQuery(id);
            if (match is not null)
            {
                return match;
            }
        }

        return null;
    }

    public bool RemoveQuery(Guid id)
    {
        var removed = Queries.RemoveAll(q => q.Id == id) > 0;
        if (removed)
        {
            return true;
        }

        return Folders.Any(folder => folder.RemoveQuery(id));
    }

    public bool RemoveFolder(Guid id)
    {
        var removed = Folders.RemoveAll(f => f.Id == id) > 0;
        if (removed)
        {
            return true;
        }

        return Folders.Any(folder => folder.RemoveFolder(id));
    }
}
