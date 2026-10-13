using OctoSql.App.Models;
using OctoSql.App.Services;

namespace OctoSql.App.Forms;

public sealed class ConnectionEditorForm : Form
{
    private readonly BindingSource _bindingSource = new();
    private readonly List<ConnectionProfile> _connections;
    private readonly SqlExecutionService _sqlService = new();

    private readonly ListBox _listBox = new() { Dock = DockStyle.Fill };
    private readonly TextBox _nameBox = new();
    private readonly TextBox _serverBox = new();
    private readonly TextBox _databaseBox = new();
    private readonly CheckBox _windowsAuthCheck = new() { Text = "Windows Authentication", AutoSize = true };
    private readonly TextBox _userBox = new();
    private readonly TextBox _passwordBox = new() { UseSystemPasswordChar = true };
    private readonly CheckBox _trustCertCheck = new() { Text = "Trust server certificate", AutoSize = true, Checked = true };
    private readonly NumericUpDown _timeoutBox = new() { Minimum = 0, Maximum = 3600, Value = 60 };
    private readonly Label _statusLabel = new() { AutoSize = true, ForeColor = Color.DimGray };

    public IReadOnlyList<ConnectionProfile> Connections => _connections;

    public ConnectionEditorForm(IEnumerable<ConnectionProfile> connections)
    {
        _connections = connections.Select(c => c.Clone()).ToList();
        Text = "Manage Connections";
        Width = 760;
        Height = 480;
        MinimumSize = new Size(680, 420);
        StartPosition = FormStartPosition.CenterParent;
        Font = new Font("Segoe UI", 9F);

        BuildLayout();
        WireEvents();

        _bindingSource.DataSource = _connections;
        _listBox.DisplayMember = nameof(ConnectionProfile.Name);
        _listBox.DataSource = _bindingSource;

        if (_connections.Count > 0)
        {
            _listBox.SelectedIndex = 0;
        }

        UpdateEditorEnabled();
    }

    private void BuildLayout()
    {
        var split = new SplitContainer
        {
            Dock = DockStyle.Fill,
            Orientation = Orientation.Vertical,
            SplitterDistance = 240
        };

        var leftPanel = new Panel { Dock = DockStyle.Fill, Padding = new Padding(8) };
        var buttonRow = new FlowLayoutPanel
        {
            Dock = DockStyle.Bottom,
            Height = 40,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = false
        };

        var addButton = new Button { Text = "Add", Width = 70 };
        var removeButton = new Button { Text = "Remove", Width = 70 };
        addButton.Click += (_, _) => AddConnection();
        removeButton.Click += (_, _) => RemoveConnection();
        buttonRow.Controls.AddRange(new Control[] { addButton, removeButton });

        leftPanel.Controls.Add(_listBox);
        leftPanel.Controls.Add(buttonRow);
        split.Panel1.Controls.Add(leftPanel);

        var details = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            RowCount = 9,
            Padding = new Padding(12),
            AutoSize = true
        };
        details.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 140));
        details.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

        void AddRow(int row, string label, Control control)
        {
            details.Controls.Add(new Label
            {
                Text = label,
                AutoSize = true,
                Anchor = AnchorStyles.Left,
                TextAlign = ContentAlignment.MiddleLeft
            }, 0, row);
            control.Dock = DockStyle.Fill;
            details.Controls.Add(control, 1, row);
        }

        AddRow(0, "Name", _nameBox);
        AddRow(1, "Server", _serverBox);
        AddRow(2, "Database", _databaseBox);
        details.Controls.Add(_windowsAuthCheck, 1, 3);
        AddRow(4, "User name", _userBox);
        AddRow(5, "Password", _passwordBox);
        details.Controls.Add(_trustCertCheck, 1, 6);
        AddRow(7, "Command timeout (s)", _timeoutBox);

        var testButton = new Button { Text = "Test Connection", AutoSize = true };
        testButton.Click += async (_, _) => await TestSelectedAsync();
        var testRow = new FlowLayoutPanel { Dock = DockStyle.Fill, FlowDirection = FlowDirection.LeftToRight };
        testRow.Controls.Add(testButton);
        testRow.Controls.Add(_statusLabel);
        details.Controls.Add(testRow, 1, 8);

        split.Panel2.Controls.Add(details);

        var footer = new FlowLayoutPanel
        {
            Dock = DockStyle.Bottom,
            Height = 48,
            FlowDirection = FlowDirection.RightToLeft,
            Padding = new Padding(8)
        };
        var okButton = new Button { Text = "OK", DialogResult = DialogResult.OK, Width = 90 };
        var cancelButton = new Button { Text = "Cancel", DialogResult = DialogResult.Cancel, Width = 90 };
        AcceptButton = okButton;
        CancelButton = cancelButton;
        footer.Controls.Add(okButton);
        footer.Controls.Add(cancelButton);

        Controls.Add(split);
        Controls.Add(footer);
    }

    private void WireEvents()
    {
        _listBox.SelectedIndexChanged += (_, _) => LoadSelectedIntoEditor();
        _nameBox.TextChanged += (_, _) =>
        {
            if (Selected is null) return;
            Selected.Name = _nameBox.Text.Trim();
            _bindingSource.ResetCurrentItem();
        };
        _serverBox.TextChanged += (_, _) => { if (Selected is not null) Selected.Server = _serverBox.Text.Trim(); };
        _databaseBox.TextChanged += (_, _) => { if (Selected is not null) Selected.Database = NullIfEmpty(_databaseBox.Text); };
        _userBox.TextChanged += (_, _) => { if (Selected is not null) Selected.UserName = NullIfEmpty(_userBox.Text); };
        _passwordBox.TextChanged += (_, _) => { if (Selected is not null) Selected.Password = _passwordBox.Text; };
        _windowsAuthCheck.CheckedChanged += (_, _) =>
        {
            if (Selected is not null)
            {
                Selected.UseWindowsAuth = _windowsAuthCheck.Checked;
            }

            UpdateAuthFields();
        };
        _trustCertCheck.CheckedChanged += (_, _) =>
        {
            if (Selected is not null)
            {
                Selected.TrustServerCertificate = _trustCertCheck.Checked;
            }
        };
        _timeoutBox.ValueChanged += (_, _) =>
        {
            if (Selected is not null)
            {
                Selected.CommandTimeoutSeconds = (int)_timeoutBox.Value;
            }
        };
    }

    private ConnectionProfile? Selected => _listBox.SelectedItem as ConnectionProfile;

    private void LoadSelectedIntoEditor()
    {
        var selected = Selected;
        UpdateEditorEnabled();
        if (selected is null)
        {
            return;
        }

        _nameBox.Text = selected.Name;
        _serverBox.Text = selected.Server;
        _databaseBox.Text = selected.Database ?? string.Empty;
        _windowsAuthCheck.Checked = selected.UseWindowsAuth;
        _userBox.Text = selected.UserName ?? string.Empty;
        _passwordBox.Text = selected.Password ?? string.Empty;
        _trustCertCheck.Checked = selected.TrustServerCertificate;
        _timeoutBox.Value = Math.Clamp(selected.CommandTimeoutSeconds, 0, 3600);
        UpdateAuthFields();
    }

    private void UpdateEditorEnabled()
    {
        var enabled = Selected is not null;
        foreach (Control control in new Control[]
                 {
                     _nameBox, _serverBox, _databaseBox, _windowsAuthCheck, _userBox, _passwordBox,
                     _trustCertCheck, _timeoutBox
                 })
        {
            control.Enabled = enabled;
        }
    }

    private void UpdateAuthFields()
    {
        var sqlAuth = !_windowsAuthCheck.Checked;
        _userBox.Enabled = Selected is not null && sqlAuth;
        _passwordBox.Enabled = Selected is not null && sqlAuth;
    }

    private void AddConnection()
    {
        var profile = new ConnectionProfile
        {
            Name = $"Connection {_connections.Count + 1}"
        };
        _connections.Add(profile);
        _bindingSource.ResetBindings(false);
        _listBox.SelectedItem = profile;
    }

    private void RemoveConnection()
    {
        if (Selected is null)
        {
            return;
        }

        _connections.Remove(Selected);
        _bindingSource.ResetBindings(false);
        UpdateEditorEnabled();
    }

    private async Task TestSelectedAsync()
    {
        if (Selected is null)
        {
            return;
        }

        _statusLabel.ForeColor = Color.DimGray;
        _statusLabel.Text = "Testing...";
        UseWaitCursor = true;
        try
        {
            var (ok, message) = await _sqlService.TestConnectionAsync(Selected);
            _statusLabel.ForeColor = ok ? Color.ForestGreen : Color.Firebrick;
            _statusLabel.Text = message;
        }
        finally
        {
            UseWaitCursor = false;
        }
    }

    private static string? NullIfEmpty(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
