# Octo SQL

A Windows Forms app for **organizing useful SQL queries** and **running them against SQL Server** with a simple, focused UX.

## Features

- **Query library** with folders, search/filter, tags, and descriptions
- **Connection profiles** (Windows auth or SQL auth, database, timeout, trust certificate)
- **SQL editor** with Run (F5) / Run Selection (Ctrl+F5) and cancel
- **Results grid** plus a messages pane for PRINT/info output and errors
- **Import `.sql` files** into the library
- **Seeded scripts** from this repo (diagnostics, restore, clone, validation)

Your library is saved to `%AppData%\OctoSql\library.json`.

## Requirements

- Windows 10/11
- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- Access to a SQL Server instance (local or remote)

## Run

```bash
dotnet restore OctoSql.sln
dotnet run --project src/OctoSql.App
```

Or open `OctoSql.sln` in Visual Studio 2022 and press F5.

## Layout

```
OctoSql.sln
src/OctoSql.App/          WinForms application
  Forms/                  Connection + query property dialogs
  Models/                 Library, folders, queries, connections
  Services/               Persistence, seeding, SQL execution
  Queries/                Seed .sql scripts copied into the build output
scripts/                  Same scripts kept at repo level for reference
```

## Typical workflow

1. **Connections → Manage Connections** — add your SQL Server (e.g. `AGF-P3SPZ-WS1`) and optional database (`TRN_CreditDeliveryDB`).
2. Pick a query from the left tree (Diagnostics / Operations / Validation).
3. Review or edit SQL in the editor.
4. Press **F5** to run against the selected connection.
5. Inspect **Results** and **Messages**.

## Notes

- Seeded `orchestrator.sql` uses SQLCMD `!!osql` syntax; run those steps from SSMS SQLCMD mode or adapt them to connections + individual scripts in Octo SQL.
- `trn_clone_data.sql` is large; the app loads it as a normal saved query.
- Passwords for SQL auth are stored in the local library JSON — prefer Windows auth when possible.
