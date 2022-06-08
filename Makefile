VIRTUALENV = $(shell which virtualenv)

ifeq ($(strip $(VIRTUALENV)),)
  VIRTUALENV = /usr/local/python3/bin/virtualenv
endif

venv:
	$(VIRTUALENV) venv

install: venv
	. venv/bin/activate; python -m pip install -U pip ; python -m pip install -e .

install-dev: venv
	. venv/bin/activate; python -m pip install -e .[dev]

install-ci: venv
	. venv/bin/activate; python -m pip install -e .[ci]


install-test: venv install
	. venv/bin/activate; python -m pip install -e .[test]


test_lite: venv install-test
	. venv/bin/activate; tox

test: venv install-test test_lite clean

release: venv
	. venv/bin/activate; python -m pip install twine
	. venv/bin/activate; twine upload dist/*


build: venv
	. venv/bin/activate; python -m setup sdist
	. venv/bin/activate; python -m pip wheel --wheel-dir=dist_wheels .



clean:
	rm -rf diycrate.egg-info
	rm -rf dist
	rm -rf dist_wheels
	rm -rf build
