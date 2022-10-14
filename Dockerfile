FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest

COPY README.md .
COPY setup.py .
COPY cpg_utils cpg_utils
RUN pip install .[workflows]
