FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest

COPY README.md .
COPY cpg_utils .
COPY setup.py .

RUN python3 -m pip install -e '.[workflows]'
