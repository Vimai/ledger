create-db:
	python cli.py create-db

drop-db:
	python cli.py drop-db

balance:
	python cli.py balances

test:
	python -m unittest tests.test_cli

load-test1:
	python cli.py load tests/test1.csv

load-test2:
	python cli.py load tests/test2.csv
