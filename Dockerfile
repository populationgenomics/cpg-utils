FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest

ADD cpg_utils .
ADD setup.py .
RUN python3 -m pip install -e '.[workflows]'
