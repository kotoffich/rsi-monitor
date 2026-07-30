@echo off
cd /d "%~dp0"
git add -A
git commit -m "Правки %date% %time%"
git push
echo.
echo Готово! Через минуту сайт обновится: https://kotoffich.github.io/rsi-monitor/
pause
