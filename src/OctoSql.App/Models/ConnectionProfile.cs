namespace OctoSql.App.Models;

public sealed class ConnectionProfile
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "New Connection";
    public string Server { get; set; } = "localhost";
    public string? Database { get; set; }
    public bool UseWindowsAuth { get; set; } = true;
    public string? UserName { get; set; }
    public string? Password { get; set; }
    public bool TrustServerCertificate { get; set; } = true;
    public int CommandTimeoutSeconds { get; set; } = 60;

    public string BuildConnectionString(bool includePassword = true)
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = Server,
            IntegratedSecurity = UseWindowsAuth,
            TrustServerCertificate = TrustServerCertificate,
            ConnectTimeout = 15,
            ApplicationName = "Octo SQL"
        };

        if (!string.IsNullOrWhiteSpace(Database))
        {
            builder.InitialCatalog = Database;
        }

        if (!UseWindowsAuth)
        {
            builder.UserID = UserName ?? string.Empty;
            if (includePassword)
            {
                builder.Password = Password ?? string.Empty;
            }
        }

        return builder.ConnectionString;
    }

    public ConnectionProfile Clone() => new()
    {
        Id = Id,
        Name = Name,
        Server = Server,
        Database = Database,
        UseWindowsAuth = UseWindowsAuth,
        UserName = UserName,
        Password = Password,
        TrustServerCertificate = TrustServerCertificate,
        CommandTimeoutSeconds = CommandTimeoutSeconds
    };
}
