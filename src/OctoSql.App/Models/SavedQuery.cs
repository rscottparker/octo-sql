namespace OctoSql.App.Models;

public sealed class SavedQuery
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "New Query";
    public string? Description { get; set; }
    public string Sql { get; set; } = string.Empty;
    public List<string> Tags { get; set; } = [];
    public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    public DateTime ModifiedUtc { get; set; } = DateTime.UtcNow;

    public SavedQuery Clone() => new()
    {
        Id = Id,
        Name = Name,
        Description = Description,
        Sql = Sql,
        Tags = [.. Tags],
        CreatedUtc = CreatedUtc,
        ModifiedUtc = ModifiedUtc
    };
}
