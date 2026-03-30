@echo off
cd /d C:\_Stock_Report
echo [%date% %time%] ?? ?? ?? >> scheduled_log.txt
python run_daily.py >> scheduled_log.txt 2>&1
echo [%date% %time%] ?? ?? ?? >> scheduled_log.txt
echo ================================ >> scheduled_log.txt
exit /b 0
