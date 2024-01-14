# "Исполняемый" файл, реализующий консольный интерфейс
# для всего функционала

import sys

def main(args):

	if not args:
		print("Possible parameters (date format YYYY-MM-DD):")
		print("-> arxiv2csv *date_from* *date_to*")
		print("-> cwp2csv")
		print("-> clear_selenium")
		print("-> csv2db *date_from* *date_to*")
		print("-> init_db")
		print("-> drop_db")
		print("-> spark *date_from* *date_to* *term1* ... [*termN*]")
		return
	
	fn = args[0]
	
	if   fn == 'arxiv2csv':
		from inner.arxiv_to_csv import run
		run(args[1], args[2])
	elif fn == 'cwp2csv':
		from inner.cwp_to_csv import run
		run()
	elif fn == 'clear_selenium':
		from inner.cwp_to_csv import purge_selenium
		purge_selenium()
	elif fn == 'csv2db':
		from inner.csv2db import run
		run(args[1], args[2])
	elif fn == 'init_db':
		from inner.csv2db import init_db
		init_db()
	elif fn == 'drop_db':
		from inner.csv2db import drop_db
		drop_db() # Чисто для теста
	elif fn == 'spark':
		from inner.spark_job import run
		run(args[1], args[2], ' '.join(args[3:]))

if __name__ == '__main__':	
    main(sys.argv[1:])

	# Примеры/сценарий использования:
r"""
	python parser.py clear_selenium
	python parser.py arxiv2csv 2023-12-19 2023-12-27
	python parser.py cwp2csv
	python parser.py init_db
	python parser.py drop_db
	python parser.py init_db
	python parser.py csv2db 2023-12-19 2023-12-27
	python parser.py spark 2023-12-19 2023-12-21 "iMAge" "\seGMentation|RObuST"
"""
