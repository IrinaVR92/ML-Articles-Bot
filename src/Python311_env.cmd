@echo off
cd \Python311
python.exe %*
copy spark_job.csv \\wsl$\Ubuntu\home\irina