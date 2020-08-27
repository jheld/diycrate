VIRTUALENV = $(shell which virtualenv)

ifeq ($(strip $(VIRTUALENV)),)
  VIRTUALENV = /usr/local/python3/bin/virtualenv
endif

venv:
	$(VIRTUALENV) venv

install: venv
	. venv/bin/activate; python -m pip install -U pip ; python -m pip install -e . --use-feature=2020-resolver

install-dev: venv
	. venv/bin/activate; python -m pip install -e .[dev] --use-feature=2020-resolver


install-test: venv install
	. venv/bin/activate; python -m pip install -e .[test] --use-feature=2020-resolver


test_lite: venv install-test
	. venv/bin/activate; tox

test: venv install-test test_lite clean

release: venv
	. venv/bin/activate; python -m pip install twine --use-feature=2020-resolver
	. venv/bin/activate; twine upload dist/*


build: venv
	. venv/bin/activate; python setup.py sdist
	. venv/bin/activate; python setup.py bdist_wheel



clean:
	rm -rf diycrate.egg-info
	rm -rf dist
	rm -rf build
