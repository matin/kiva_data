SHELL := bash
PATH := ./venv/bin:${PATH}
PYTHON=python3.6


default: install-dev

etl:
		$(PYTHON) -m kiva.etl

venv:
		$(PYTHON) -m venv --prompt kiva_data venv

install: venv
		pip install --quiet --upgrade -r requirements.txt

install-dev: install
		pip install --quiet --upgrade -r requirements-dev.txt

pep8:
		pep8 kiva/

lint: pep8