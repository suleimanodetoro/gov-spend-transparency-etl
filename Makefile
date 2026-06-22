.PHONY: setup data run rerun test clean
PY=python3

setup:
	pip install -r requirements.txt

data:
	$(PY) generate_sample_data.py

run:
	$(PY) src/pipeline.py

rerun: run   ## run again; already-processed files are skipped

test:
	$(PY) -m pytest -q tests/

clean:
	rm -rf output
