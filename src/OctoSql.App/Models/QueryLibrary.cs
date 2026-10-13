namespace OctoSql.App.Models;

public sealed class QueryLibrary
{
    public int Version { get; set; } = 1;
    public List<ConnectionProfile> Connections { get; set; } = [];
    public QueryFolder Root { get; set; } = new() { Name = "Library" };
    public Guid? LastConnectionId { get; set; }
}
