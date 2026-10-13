using System.Data;
using OctoSql.App.Forms;
using OctoSql.App.Models;
using OctoSql.App.Services;

namespace OctoSql.App;

public sealed class MainForm : Form
{
    private readonly LibraryStore _store = new();
    private readonly SqlExecutionService _sqlService = new();
    private QueryLibrary _library = null!;
    private SavedQuery? _currentQuery;
    private bool _dirty;
    private CancellationTokenSource? _runCts;

    private readonly TreeView _libraryTree = new()
    {
        Dock = DockStyle.Fill,
        HideSelection = false,
        ShowNodeToolTips = true
    };
    private readonly TextBox _searchBox = new() { PlaceholderText = "Filter queries..." };
    private readonly ComboBox _connectionCombo = new()
    {
        DropDownStyle = ComboBoxStyle.DropDownList,
        Dock = DockStyle.Fill
    };
    private readonly TextBox _sqlEditor = new()
    {
        Dock = DockStyle.Fill,
        Multiline = true,
        ScrollBars = ScrollBars.Both,
        Font = new Font("Consolas", 10.5F),
        AcceptsTab = true,
        WordWrap = false
    };
    private readonly TabControl _outputTabs = new() { Dock = DockStyle.Fill };
    private readonly DataGridView _resultsGrid = new()
    {
        Dock = DockStyle.Fill,
        ReadOnly = true,
        AllowUserToAddRows = false,
        AllowUserToDeleteRows = false,
        AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.DisplayedCells,
        BackgroundColor = Color.White
    };
    private readonly TextBox _messagesBox = new()
    {
        Dock = DockStyle.Fill,
        Multiline = true,
        ReadOnly = true,
        ScrollBars = ScrollBars.Vertical,
        Font = new Font("Consolas", 9.5F)
    };
    private readonly StatusStrip _statusStrip = new();
    private readonly ToolStripStatusLabel _statusLabel = new() { Spring = true, TextAlign = ContentAlignment.MiddleLeft };
    private readonly ToolStripStatusLabel _timingLabel = new() { Text = string.Empty };
    private readonly Label _queryTitleLabel = new()
    {
        AutoSize = true,
        Font = new Font("Segoe UI Semibold", 11F),
        Text = "Select a query"
    };
    private readonly Label _queryDescriptionLabel = new()
    {
        AutoSize = true,
        ForeColor = Color.DimGray,
        Text = string.Empty
    };
    private Button _runButton = null!;
    private Button _cancelButton = null!;

    public MainForm()
    {
        Text = "Octo SQL";
        Width = 1280;
        Height = 800;
        MinimumSize = new Size(960, 600);
        StartPosition = FormStartPosition.CenterScreen;
        Font = new Font("Segoe UI", 9F);

        _library = _store.LoadOrCreate();
        BuildMenu();
        BuildLayout();
        WireEvents();
        RefreshConnections();
        RefreshLibraryTree();
        UpdateUiState();
        AppendMessage($"Library loaded from {_store.LibraryPath}");
    }

    private void BuildMenu()
    {
        var menu = new MenuStrip();

        var fileMenu = new ToolStripMenuItem("&File");
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("&New Query", null, (_, _) => NewQuery()) { ShortcutKeys = Keys.Control | Keys.N });
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("New &Folder", null, (_, _) => NewFolder()));
        fileMenu.DropDownItems.Add(new ToolStripSeparator());
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("&Save", null, (_, _) => SaveCurrent()) { ShortcutKeys = Keys.Control | Keys.S });
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("Save &Library", null, (_, _) => SaveLibrary()));
        fileMenu.DropDownItems.Add(new ToolStripSeparator());
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("&Import .sql File...", null, (_, _) => ImportSqlFile()));
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("Open Library &Folder", null, (_, _) =>
        {
            var dir = Path.GetDirectoryName(_store.LibraryPath);
            if (!string.IsNullOrWhiteSpace(dir) && Directory.Exists(dir))
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = dir,
                    UseShellExecute = true
                });
            }
        }));
        fileMenu.DropDownItems.Add(new ToolStripSeparator());
        fileMenu.DropDownItems.Add(new ToolStripMenuItem("E&xit", null, (_, _) => Close()));

        var connectionMenu = new ToolStripMenuItem("&Connections");
        connectionMenu.DropDownItems.Add(new ToolStripMenuItem("&Manage Connections...", null, (_, _) => ManageConnections()));
        connectionMenu.DropDownItems.Add(new ToolStripMenuItem("&Test Selected", null, async (_, _) => await TestConnectionAsync()));

        var queryMenu = new ToolStripMenuItem("&Query");
        queryMenu.DropDownItems.Add(new ToolStripMenuItem("&Run", null, async (_, _) => await RunAsync()) { ShortcutKeys = Keys.F5 });
        queryMenu.DropDownItems.Add(new ToolStripMenuItem("Run &Selection", null, async (_, _) => await RunAsync(selectionOnly: true)) { ShortcutKeys = Keys.Control | Keys.F5 });
        queryMenu.DropDownItems.Add(new ToolStripSeparator());
        queryMenu.DropDownItems.Add(new ToolStripMenuItem("&Properties...", null, (_, _) => EditQueryProperties()));
        queryMenu.DropDownItems.Add(new ToolStripMenuItem("&Delete", null, (_, _) => DeleteSelected()) { ShortcutKeys = Keys.Delete });

        var helpMenu = new ToolStripMenuItem("&Help");
        helpMenu.DropDownItems.Add(new ToolStripMenuItem("&About Octo SQL", null, (_, _) =>
        {
            MessageBox.Show(
                this,
                "Octo SQL\nOrganize useful queries and run them against SQL Server.\n\nLibrary file:\n" + _store.LibraryPath,
                "About",
                MessageBoxButtons.OK,
                MessageBoxIcon.Information);
        }));

        menu.Items.AddRange(new ToolStripItem[] { fileMenu, connectionMenu, queryMenu, helpMenu });
        MainMenuStrip = menu;
        Controls.Add(menu);
    }

    private void BuildLayout()
    {
        var root = new SplitContainer
        {
            Dock = DockStyle.Fill,
            Orientation = Orientation.Vertical,
            SplitterDistance = 300
        };

        // Left: library
        var left = new Panel { Dock = DockStyle.Fill, Padding = new Padding(8, 4, 4, 8) };
        var searchPanel = new Panel { Dock = DockStyle.Top, Height = 32, Padding = new Padding(0, 0, 0, 6) };
        _searchBox.Dock = DockStyle.Fill;
        searchPanel.Controls.Add(_searchBox);

        var libraryButtons = new FlowLayoutPanel
        {
            Dock = DockStyle.Bottom,
            Height = 36,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = false
        };
        var newQueryBtn = new Button { Text = "New Query", AutoSize = true };
        var newFolderBtn = new Button { Text = "New Folder", AutoSize = true };
        newQueryBtn.Click += (_, _) => NewQuery();
        newFolderBtn.Click += (_, _) => NewFolder();
        libraryButtons.Controls.AddRange(new Control[] { newQueryBtn, newFolderBtn });

        left.Controls.Add(_libraryTree);
        left.Controls.Add(libraryButtons);
        left.Controls.Add(searchPanel);
        root.Panel1.Controls.Add(left);

        // Right: editor + results
        var rightSplit = new SplitContainer
        {
            Dock = DockStyle.Fill,
            Orientation = Orientation.Horizontal,
            SplitterDistance = 420
        };

        var editorHost = new Panel { Dock = DockStyle.Fill, Padding = new Padding(4, 4, 8, 4) };
        var toolbar = new TableLayoutPanel
        {
            Dock = DockStyle.Top,
            Height = 40,
            ColumnCount = 5,
            Padding = new Padding(0, 0, 0, 6)
        };
        toolbar.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 80));
        toolbar.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        toolbar.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 90));
        toolbar.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 90));
        toolbar.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 100));

        toolbar.Controls.Add(new Label
        {
            Text = "Connection",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            TextAlign = ContentAlignment.MiddleLeft
        }, 0, 0);
        toolbar.Controls.Add(_connectionCombo, 1, 0);

        var manageBtn = new Button { Text = "Manage...", Dock = DockStyle.Fill };
        manageBtn.Click += (_, _) => ManageConnections();
        _runButton = new Button { Text = "Run (F5)", Dock = DockStyle.Fill };
        _cancelButton = new Button { Text = "Cancel", Dock = DockStyle.Fill, Enabled = false };
        _runButton.Click += async (_, _) => await RunAsync();
        _cancelButton.Click += (_, _) => _runCts?.Cancel();
        toolbar.Controls.Add(manageBtn, 2, 0);
        toolbar.Controls.Add(_runButton, 3, 0);
        toolbar.Controls.Add(_cancelButton, 4, 0);

        var header = new Panel { Dock = DockStyle.Top, Height = 48, Padding = new Padding(0, 0, 0, 4) };
        var headerStack = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.TopDown,
            WrapContents = false
        };
        headerStack.Controls.Add(_queryTitleLabel);
        headerStack.Controls.Add(_queryDescriptionLabel);
        header.Controls.Add(headerStack);

        editorHost.Controls.Add(_sqlEditor);
        editorHost.Controls.Add(header);
        editorHost.Controls.Add(toolbar);

        var resultsTab = new TabPage("Results");
        resultsTab.Controls.Add(_resultsGrid);
        var messagesTab = new TabPage("Messages");
        messagesTab.Controls.Add(_messagesBox);
        _outputTabs.TabPages.Add(resultsTab);
        _outputTabs.TabPages.Add(messagesTab);

        rightSplit.Panel1.Controls.Add(editorHost);
        rightSplit.Panel2.Controls.Add(_outputTabs);
        root.Panel2.Controls.Add(rightSplit);

        _statusStrip.Items.AddRange(new ToolStripItem[] { _statusLabel, _timingLabel });
        _statusLabel.Text = "Ready";

        Controls.Add(root);
        Controls.Add(_statusStrip);
    }

    private void WireEvents()
    {
        _libraryTree.AfterSelect += (_, _) => OnTreeSelect();
        _libraryTree.NodeMouseDoubleClick += (_, e) =>
        {
            if (e.Node?.Tag is SavedQuery)
            {
                _sqlEditor.Focus();
            }
        };
        _searchBox.TextChanged += (_, _) => RefreshLibraryTree(preserveSelection: true);
        _sqlEditor.TextChanged += (_, _) =>
        {
            if (_currentQuery is null)
            {
                return;
            }

            _dirty = true;
            UpdateUiState();
        };
        FormClosing += (_, e) =>
        {
            if (!ConfirmDiscardIfNeeded())
            {
                e.Cancel = true;
                return;
            }

            SaveLibrary(silent: true);
        };

        var treeMenu = new ContextMenuStrip();
        treeMenu.Items.Add("New Query", null, (_, _) => NewQuery());
        treeMenu.Items.Add("New Folder", null, (_, _) => NewFolder());
        treeMenu.Items.Add(new ToolStripSeparator());
        treeMenu.Items.Add("Properties...", null, (_, _) => EditQueryProperties());
        treeMenu.Items.Add("Delete", null, (_, _) => DeleteSelected());
        _libraryTree.ContextMenuStrip = treeMenu;
    }

    private void RefreshConnections()
    {
        var selectedId = (_connectionCombo.SelectedItem as ConnectionProfile)?.Id ?? _library.LastConnectionId;
        _connectionCombo.DataSource = null;
        _connectionCombo.DisplayMember = nameof(ConnectionProfile.Name);
        _connectionCombo.DataSource = _library.Connections.ToList();

        if (selectedId is Guid id)
        {
            var match = _library.Connections.FirstOrDefault(c => c.Id == id);
            if (match is not null)
            {
                _connectionCombo.SelectedItem = match;
            }
        }
    }

    private void RefreshLibraryTree(bool preserveSelection = false)
    {
        Guid? selectedId = null;
        if (preserveSelection)
        {
            selectedId = _libraryTree.SelectedNode?.Tag switch
            {
                SavedQuery q => q.Id,
                QueryFolder f => f.Id,
                _ => null
            };
        }

        _libraryTree.BeginUpdate();
        _libraryTree.Nodes.Clear();

        var filter = _searchBox.Text.Trim();
        var rootNode = CreateFolderNode(_library.Root, filter);
        _libraryTree.Nodes.Add(rootNode);
        rootNode.Expand();

        if (selectedId is Guid id)
        {
            var node = FindNodeById(_libraryTree.Nodes, id);
            if (node is not null)
            {
                _libraryTree.SelectedNode = node;
                node.EnsureVisible();
            }
        }

        _libraryTree.EndUpdate();
    }

    private TreeNode CreateFolderNode(QueryFolder folder, string filter)
    {
        var node = new TreeNode(folder.Name)
        {
            Tag = folder,
            ToolTipText = "Folder"
        };

        foreach (var child in folder.Folders.OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase))
        {
            var childNode = CreateFolderNode(child, filter);
            if (string.IsNullOrWhiteSpace(filter) || childNode.Nodes.Count > 0)
            {
                node.Nodes.Add(childNode);
            }
        }

        foreach (var query in folder.Queries.OrderBy(q => q.Name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesFilter(query, filter))
            {
                continue;
            }

            node.Nodes.Add(new TreeNode(query.Name)
            {
                Tag = query,
                ToolTipText = query.Description ?? query.Name
            });
        }

        return node;
    }

    private static bool MatchesFilter(SavedQuery query, string filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
        {
            return true;
        }

        return query.Name.Contains(filter, StringComparison.OrdinalIgnoreCase)
               || (query.Description?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false)
               || query.Tags.Any(t => t.Contains(filter, StringComparison.OrdinalIgnoreCase))
               || query.Sql.Contains(filter, StringComparison.OrdinalIgnoreCase);
    }

    private static TreeNode? FindNodeById(TreeNodeCollection nodes, Guid id)
    {
        foreach (TreeNode node in nodes)
        {
            var match = node.Tag switch
            {
                SavedQuery q when q.Id == id => node,
                QueryFolder f when f.Id == id => node,
                _ => null
            };
            if (match is not null)
            {
                return match;
            }

            var nested = FindNodeById(node.Nodes, id);
            if (nested is not null)
            {
                return nested;
            }
        }

        return null;
    }

    private void OnTreeSelect()
    {
        if (_libraryTree.SelectedNode?.Tag is not SavedQuery query)
        {
            return;
        }

        if (_currentQuery?.Id == query.Id)
        {
            return;
        }

        if (!ConfirmDiscardIfNeeded())
        {
            // Re-select previous
            if (_currentQuery is not null)
            {
                var previous = FindNodeById(_libraryTree.Nodes, _currentQuery.Id);
                if (previous is not null)
                {
                    _libraryTree.SelectedNode = previous;
                }
            }

            return;
        }

        LoadQuery(query);
    }

    private void LoadQuery(SavedQuery query)
    {
        _currentQuery = query;
        _queryTitleLabel.Text = query.Name;
        _queryDescriptionLabel.Text = query.Description ?? string.Empty;
        _sqlEditor.Text = query.Sql;
        _dirty = false;
        UpdateUiState();
    }

    private bool ConfirmDiscardIfNeeded()
    {
        if (!_dirty)
        {
            return true;
        }

        var result = MessageBox.Show(
            this,
            "Save changes to the current query?",
            Text,
            MessageBoxButtons.YesNoCancel,
            MessageBoxIcon.Question);

        if (result == DialogResult.Cancel)
        {
            return false;
        }

        if (result == DialogResult.Yes)
        {
            SaveCurrent();
        }
        else
        {
            _dirty = false;
        }

        return true;
    }

    private QueryFolder GetTargetFolder()
    {
        return _libraryTree.SelectedNode?.Tag switch
        {
            QueryFolder folder => folder,
            SavedQuery query => FindParentFolder(query) ?? _library.Root,
            _ => _library.Root
        };
    }

    private QueryFolder? FindParentFolder(SavedQuery query)
    {
        return FindParentFolder(_library.Root, query);
    }

    private static QueryFolder? FindParentFolder(QueryFolder folder, SavedQuery query)
    {
        if (folder.Queries.Any(q => q.Id == query.Id))
        {
            return folder;
        }

        foreach (var child in folder.Folders)
        {
            var match = FindParentFolder(child, query);
            if (match is not null)
            {
                return match;
            }
        }

        return null;
    }

    private void NewQuery()
    {
        if (!ConfirmDiscardIfNeeded())
        {
            return;
        }

        var folder = GetTargetFolder();
        var query = new SavedQuery
        {
            Name = "New Query",
            Sql = "SELECT 1 AS [Value];"
        };

        using var props = new QueryPropertiesForm(query);
        if (props.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        query.Name = props.QueryName;
        query.Description = props.Description;
        query.Tags = props.Tags;
        folder.Queries.Add(query);
        SaveLibrary(silent: true);
        RefreshLibraryTree();
        var node = FindNodeById(_libraryTree.Nodes, query.Id);
        if (node is not null)
        {
            _libraryTree.SelectedNode = node;
        }

        LoadQuery(query);
    }

    private void NewFolder()
    {
        var parent = GetTargetFolder();
        var name = PromptForName("New Folder", "Folder name:", "New Folder");
        if (name is null)
        {
            return;
        }

        var folder = new QueryFolder { Name = name };
        parent.Folders.Add(folder);
        SaveLibrary(silent: true);
        RefreshLibraryTree();
        var node = FindNodeById(_libraryTree.Nodes, folder.Id);
        if (node is not null)
        {
            _libraryTree.SelectedNode = node;
            node.Expand();
        }
    }

    private void EditQueryProperties()
    {
        if (_currentQuery is null)
        {
            return;
        }

        using var props = new QueryPropertiesForm(_currentQuery);
        if (props.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        _currentQuery.Name = props.QueryName;
        _currentQuery.Description = props.Description;
        _currentQuery.Tags = props.Tags;
        _currentQuery.ModifiedUtc = DateTime.UtcNow;
        _queryTitleLabel.Text = _currentQuery.Name;
        _queryDescriptionLabel.Text = _currentQuery.Description ?? string.Empty;
        SaveLibrary(silent: true);
        RefreshLibraryTree(preserveSelection: true);
    }

    private void DeleteSelected()
    {
        if (_libraryTree.SelectedNode?.Tag is SavedQuery query)
        {
            if (MessageBox.Show(this, $"Delete query '{query.Name}'?", Text, MessageBoxButtons.YesNo, MessageBoxIcon.Warning) != DialogResult.Yes)
            {
                return;
            }

            _library.Root.RemoveQuery(query.Id);
            if (_currentQuery?.Id == query.Id)
            {
                _currentQuery = null;
                _sqlEditor.Clear();
                _queryTitleLabel.Text = "Select a query";
                _queryDescriptionLabel.Text = string.Empty;
                _dirty = false;
            }

            SaveLibrary(silent: true);
            RefreshLibraryTree();
            UpdateUiState();
            return;
        }

        if (_libraryTree.SelectedNode?.Tag is QueryFolder folder && folder.Id != _library.Root.Id)
        {
            if (MessageBox.Show(this, $"Delete folder '{folder.Name}' and its contents?", Text, MessageBoxButtons.YesNo, MessageBoxIcon.Warning) != DialogResult.Yes)
            {
                return;
            }

            _library.Root.RemoveFolder(folder.Id);
            SaveLibrary(silent: true);
            RefreshLibraryTree();
        }
    }

    private void SaveCurrent()
    {
        if (_currentQuery is null)
        {
            return;
        }

        _currentQuery.Sql = _sqlEditor.Text;
        _currentQuery.ModifiedUtc = DateTime.UtcNow;
        _dirty = false;
        SaveLibrary(silent: true);
        UpdateUiState();
        _statusLabel.Text = $"Saved '{_currentQuery.Name}'";
    }

    private void SaveLibrary(bool silent = false)
    {
        if (_connectionCombo.SelectedItem is ConnectionProfile selected)
        {
            _library.LastConnectionId = selected.Id;
        }

        _store.Save(_library);
        if (!silent)
        {
            _statusLabel.Text = "Library saved";
            AppendMessage($"Saved library to {_store.LibraryPath}");
        }
    }

    private void ManageConnections()
    {
        using var form = new ConnectionEditorForm(_library.Connections);
        if (form.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        _library.Connections = form.Connections.ToList();
        SaveLibrary(silent: true);
        RefreshConnections();
    }

    private async Task TestConnectionAsync()
    {
        if (_connectionCombo.SelectedItem is not ConnectionProfile connection)
        {
            MessageBox.Show(this, "Select a connection first.", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        _statusLabel.Text = "Testing connection...";
        var (ok, message) = await _sqlService.TestConnectionAsync(connection);
        _statusLabel.Text = message;
        AppendMessage(ok ? $"OK: {message}" : $"FAILED: {message}");
        _outputTabs.SelectedIndex = 1;
        if (!ok)
        {
            MessageBox.Show(this, message, "Connection failed", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task RunAsync(bool selectionOnly = false)
    {
        if (_connectionCombo.SelectedItem is not ConnectionProfile connection)
        {
            MessageBox.Show(this, "Select a connection first.", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        var sql = selectionOnly && _sqlEditor.SelectionLength > 0
            ? _sqlEditor.SelectedText
            : _sqlEditor.Text;

        if (string.IsNullOrWhiteSpace(sql))
        {
            MessageBox.Show(this, "Nothing to execute.", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        if (_dirty)
        {
            SaveCurrent();
        }

        _runCts = new CancellationTokenSource();
        _runButton.Enabled = false;
        _cancelButton.Enabled = true;
        _statusLabel.Text = "Executing...";
        _timingLabel.Text = string.Empty;
        UseWaitCursor = true;

        try
        {
            var result = await _sqlService.ExecuteAsync(connection, sql, _runCts.Token);
            ShowExecutionResult(result);
        }
        finally
        {
            UseWaitCursor = false;
            _runButton.Enabled = true;
            _cancelButton.Enabled = false;
            _runCts.Dispose();
            _runCts = null;
            UpdateUiState();
        }
    }

    private void ShowExecutionResult(QueryExecutionResult result)
    {
        _timingLabel.Text = $"{result.Duration.TotalMilliseconds:0} ms";
        _statusLabel.Text = result.Succeeded ? result.Message : "Execution failed";

        _messagesBox.Clear();
        AppendMessage(result.Succeeded ? $"Success: {result.Message}" : $"Error: {result.Message}");
        foreach (var info in result.InfoMessages)
        {
            AppendMessage(info);
        }

        _resultsGrid.DataSource = null;
        if (result.Data is { Tables.Count: > 0 })
        {
            // Show the first result set; additional sets are noted in messages.
            _resultsGrid.DataSource = result.Data.Tables[0];
            if (result.Data.Tables.Count > 1)
            {
                AppendMessage($"Note: {result.Data.Tables.Count} result sets returned; showing the first.");
            }

            _outputTabs.SelectedIndex = 0;
        }
        else
        {
            _outputTabs.SelectedIndex = 1;
        }
    }

    private void ImportSqlFile()
    {
        using var dialog = new OpenFileDialog
        {
            Filter = "SQL files (*.sql)|*.sql|All files (*.*)|*.*",
            Multiselect = true,
            Title = "Import SQL files"
        };

        if (dialog.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        var folder = GetTargetFolder();
        foreach (var path in dialog.FileNames)
        {
            var sql = File.ReadAllText(path);
            var query = new SavedQuery
            {
                Name = Path.GetFileNameWithoutExtension(path),
                Description = $"Imported from {Path.GetFileName(path)}",
                Sql = sql,
                Tags = ["imported"]
            };
            folder.Queries.Add(query);
        }

        SaveLibrary(silent: true);
        RefreshLibraryTree();
        AppendMessage($"Imported {dialog.FileNames.Length} file(s) into '{folder.Name}'.");
    }

    private void UpdateUiState()
    {
        var title = "Octo SQL";
        if (_currentQuery is not null)
        {
            title += $" — {_currentQuery.Name}";
            if (_dirty)
            {
                title += " *";
            }
        }

        Text = title;
        _runButton.Enabled = _runCts is null;
    }

    private void AppendMessage(string message)
    {
        var line = $"[{DateTime.Now:HH:mm:ss}] {message}{Environment.NewLine}";
        _messagesBox.AppendText(line);
    }

    private string? PromptForName(string title, string label, string defaultValue)
    {
        using var form = new Form
        {
            Text = title,
            Width = 420,
            Height = 160,
            FormBorderStyle = FormBorderStyle.FixedDialog,
            StartPosition = FormStartPosition.CenterParent,
            MaximizeBox = false,
            MinimizeBox = false
        };

        var prompt = new Label { Text = label, Left = 12, Top = 16, AutoSize = true };
        var box = new TextBox { Left = 12, Top = 40, Width = 380, Text = defaultValue };
        var ok = new Button { Text = "OK", DialogResult = DialogResult.OK, Left = 216, Top = 80, Width = 85 };
        var cancel = new Button { Text = "Cancel", DialogResult = DialogResult.Cancel, Left = 307, Top = 80, Width = 85 };
        form.Controls.AddRange(new Control[] { prompt, box, ok, cancel });
        form.AcceptButton = ok;
        form.CancelButton = cancel;

        return form.ShowDialog(this) == DialogResult.OK && !string.IsNullOrWhiteSpace(box.Text)
            ? box.Text.Trim()
            : null;
    }
}
