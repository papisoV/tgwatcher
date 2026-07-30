Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd.exe /c node ""C:\Users\Jearko\.claude\glm-proxy\proxy.mjs"" >> ""C:\Users\Jearko\.claude\glm-proxy\proxy.log"" 2>&1", 1, False
