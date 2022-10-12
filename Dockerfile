FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:62e79e54d43aa1762ec68a6661872320a39517bd-hail-2ea2615a797a5aff72d20d9d12a2609342846a07

ADD cpg_utils .
ADD setup.py .
RUN python3 -m pip install .[workflows]
