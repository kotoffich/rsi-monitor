@echo off
cd /d "%~dp0"
git add -A
git diff --cached --quiet
if %errorlevel%==0 (
  echo No new changes - GitHub is already up to date.
) else (
  git commit -m "Update %date% %time%"
)
git push
echo.
echo Done! Site updates in ~1 minute: https://kotoffich.github.io/rsi-monitor/
pause
