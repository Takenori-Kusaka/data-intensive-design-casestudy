param([string]$xlsx)
# Open the workbook in Excel (COM), recalculate, and report: sheets, the formula results of the sheet 集計,
# blank cells of the sheet 一覧 outside the entry columns, and whether the entry columns are empty.
# Pass an absolute path with backslashes.
$ErrorActionPreference = 'Stop'
$xl = New-Object -ComObject Excel.Application
$xl.DisplayAlerts = $false
try {
  $wb = $xl.Workbooks.Open($xlsx, 0, $true)
  "sheets: " + (($wb.Worksheets | ForEach-Object { $_.Name }) -join ', ')
  $xl.CalculateFull()
  $ws = $wb.Worksheets.Item('集計')
  $used = $ws.UsedRange.Rows.Count
  $errors = 0
  for ($r = 1; $r -le $used; $r++) {
    $a = $ws.Cells.Item($r, 1).Text; $b = $ws.Cells.Item($r, 2)
    if ($b.HasFormula) {
      if ($b.Text -like '#*') { $errors++ }
      "  {0} = {1}" -f $a, $b.Text
    } elseif ($a -and -not $ws.Cells.Item($r, 2).Text) { "[$a]" }
  }
  "formula errors: $errors"
  $main = $wb.Worksheets.Item('一覧')
  $rows = $main.UsedRange.Rows.Count; $cols = $main.UsedRange.Columns.Count
  $v = $main.Range($main.Cells.Item(2, 1), $main.Cells.Item($rows, $cols)).Value2
  $entry = @(); for ($c = 1; $c -le $cols; $c++) { if ($main.Cells.Item(1, $c).MergeArea.Cells.Item(1, 1).Text -like '記入欄*') { $entry += $c } }
  $blank = 0; $filled = 0
  for ($r = 2; $r -le ($rows - 1); $r++) { for ($c = 1; $c -le $cols; $c++) {
    $x = $v[$r, $c]; $empty = ($null -eq $x) -or ("$x".Trim() -eq '')
    if ($entry -contains $c) { if (-not $empty) { $filled++ } } elseif ($empty) { $blank++ }
  } }
  "一覧: data rows $($rows - 2), columns $cols, entry columns $($entry.Count)"
  "blank cells outside the entry columns: $blank"
  "filled cells inside the entry columns: $filled"
  "hyperlinks: $($main.Hyperlinks.Count); autofilter: $($main.AutoFilterMode)"
  $wb.Close($false)
} finally { $xl.Quit(); [void][Runtime.InteropServices.Marshal]::ReleaseComObject($xl) }
