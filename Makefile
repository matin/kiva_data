SHELL := bash
PATH := ./venv/bin:${PATH}
PYTHON=python3.6


venv:
		$(PYTHON) -m venv --prompt kiva_data venv

install: venv
		pip install --quiet --upgrade -r requirements.txt


.PHONY: install