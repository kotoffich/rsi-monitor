@echo off
chcp 65001 >nul
cd /d "%~dp0"
git add -A
git diff --cached --quiet
if %errorlevel%==0 (
  echo Новых правок нет — на GitHub уже самая свежая версия.
) else (
  git commit -m "Правки %date% %time%"
)
git push
echo.
echo Готово! Через минуту сайт обновится: https://kotoffich.github.io/rsi-monitor/
pause
