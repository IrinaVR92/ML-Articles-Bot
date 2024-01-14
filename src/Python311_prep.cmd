@echo off
cd \Python311
Scripts\pip install pyspark psycopg2 selenium beautifulsoup4 requests pandas
mkdir inner
copy \\wsl$\Ubuntu\home\irina\db_settings.inf .
copy \\wsl$\Ubuntu\home\irina\inner .\inner
copy \\wsl$\Ubuntu\home\irina\parser.py .