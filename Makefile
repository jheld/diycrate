VIRTUALENV = $(shell which virtualenv)

ifeq ($(strip $(VIRTUALENV)),)
  VIRTUALENV = /usr/local/python3/bin/virtualenv
endif

venv:
	$(VIRTUALENV) venv

install: venv
	. venv/bin/activate; python -m pip install -e .

install-dev: venv
	. venv/bin/activate; python -m pip install -e .[dev]


install-test: venv install
	. venv/bin/activate; python -m pip install -e .[test]


test_lite: venv install-test
	. venv/bin/activate; tox

test: venv install-test test_lite clean

release: venv
	. venv/bin/activate; python -m pip install twine
	. venv/bin/activate; twine upload dist/*


build: venv
	. venv/bin/activate; python setup.py sdist
	. venv/bin/activate; python setup.py bdist_wheel



clean:
	rm -rf diycrate.egg-info
	rm -rf dist
	rm -rf build
