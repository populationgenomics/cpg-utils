FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest

COPY README.md LICENSE ./
COPY pyproject.toml .
COPY cpg_utils cpg_utils
RUN pip install .
