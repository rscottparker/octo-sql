using OctoSql.App.Models;

namespace OctoSql.App.Forms;

public sealed class QueryPropertiesForm : Form
{
    private readonly TextBox _nameBox = new();
    private readonly TextBox _descriptionBox = new() { Multiline = true, Height = 80, AcceptsReturn = true };
    private readonly TextBox _tagsBox = new();

    public string QueryName => _nameBox.Text.Trim();
    public string? Description => string.IsNullOrWhiteSpace(_descriptionBox.Text) ? null : _descriptionBox.Text.Trim();
    public List<string> Tags => _tagsBox.Text
        .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToList();

    public QueryPropertiesForm(SavedQuery query)
    {
        Text = "Query Properties";
        Width = 480;
        Height = 300;
        FormBorderStyle = FormBorderStyle.FixedDialog;
        MaximizeBox = false;
        MinimizeBox = false;
        StartPosition = FormStartPosition.CenterParent;
        Font = new Font("Segoe UI", 9F);

        var layout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            RowCount = 4,
            Padding = new Padding(12)
        };
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 100));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

        _nameBox.Text = query.Name;
        _descriptionBox.Text = query.Description ?? string.Empty;
        _tagsBox.Text = string.Join(", ", query.Tags);

        layout.Controls.Add(new Label { Text = "Name", AutoSize = true }, 0, 0);
        layout.Controls.Add(_nameBox, 1, 0);
        _nameBox.Dock = DockStyle.Fill;

        layout.Controls.Add(new Label { Text = "Description", AutoSize = true }, 0, 1);
        layout.Controls.Add(_descriptionBox, 1, 1);
        _descriptionBox.Dock = DockStyle.Fill;

        layout.Controls.Add(new Label { Text = "Tags", AutoSize = true }, 0, 2);
        layout.Controls.Add(_tagsBox, 1, 2);
        _tagsBox.Dock = DockStyle.Fill;
        layout.Controls.Add(new Label
        {
            Text = "Comma-separated",
            AutoSize = true,
            ForeColor = Color.DimGray
        }, 1, 3);

        var footer = new FlowLayoutPanel
        {
            Dock = DockStyle.Bottom,
            Height = 48,
            FlowDirection = FlowDirection.RightToLeft,
            Padding = new Padding(8)
        };
        var ok = new Button { Text = "OK", DialogResult = DialogResult.OK, Width = 90 };
        var cancel = new Button { Text = "Cancel", DialogResult = DialogResult.Cancel, Width = 90 };
        ok.Click += (_, _) =>
        {
            if (string.IsNullOrWhiteSpace(_nameBox.Text))
            {
                MessageBox.Show(this, "Name is required.", Text, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                DialogResult = DialogResult.None;
            }
        };
        AcceptButton = ok;
        CancelButton = cancel;
        footer.Controls.Add(ok);
        footer.Controls.Add(cancel);

        Controls.Add(layout);
        Controls.Add(footer);
    }
}
