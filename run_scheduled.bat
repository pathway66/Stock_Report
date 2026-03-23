@echo off
cd /d C:\_Stock_Report
echo [%date% %time%] 자동 수집 시작 >> scheduled_log.txt
python run_daily.py >> scheduled_log.txt 2>&1
echo [%date% %time%] 자동 수집 완료 >> scheduled_log.txt
echo ================================ >> scheduled_log.txt
