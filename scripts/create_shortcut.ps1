$ws = New-Object -ComObject WScript.Shell
$desktop = [Environment]::GetFolderPath('Desktop')
$lnk = Join-Path $desktop 'TGWatcher.lnk'
$s = $ws.CreateShortcut($lnk)
$s.TargetPath = 'D:\PROJECT_OC\tgwatcher\start.bat'
$s.WorkingDirectory = 'D:\PROJECT_OC\tgwatcher'
$s.IconLocation = 'C:\Windows\System32\shell32.dll,13'
$s.Description = 'TGWatcher - Telegram Group Crawler'
$s.WindowStyle = 3
$s.Save()
Write-Output "Created: $lnk"
