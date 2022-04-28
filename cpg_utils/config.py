import json
import logging
from os import getenv
from typing import Any, Dict, Optional

deploy_config: "DeployConfig" = None
server_config: Dict[str, Any] = None
DEFAULT_CONFIG = {
    "cloud": "gcp",
    "sample_metadata_project": "sample-metadata",
    "sample_metadata_host": "http://localhost:8000",
    "analysis_runner_project": "analysis-runner",
    "analysis_runner_host": "http://localhost:8001",
}


class DeployConfig:
    @staticmethod
    def from_dict(config: Dict[str, str]) -> "DeployConfig":
        return DeployConfig(**config)

    @staticmethod
    def from_environment() -> "DeployConfig":
        deploy_config = json.loads(getenv("CPG_DEPLOY_CONFIG", json.dumps(DEFAULT_CONFIG)))
        # Allow individual field overrides.
        deploy_config["cloud"] = getenv("CLOUD", deploy_config["cloud"])
        deploy_config["sample_metadata_host"] = getenv("SM_HOST_URL", deploy_config["sample_metadata_host"])
        return DeployConfig.from_dict(deploy_config)

    def __init__(
        self,
        cloud: Optional[str],
        sample_metadata_project: Optional[str],
        sample_metadata_host: Optional[str],
        analysis_runner_project: Optional[str],
        analysis_runner_host: Optional[str],
    ):
        self.cloud = cloud or DEFAULT_CONFIG["cloud"]
        self.sample_metadata_project = sample_metadata_project or DEFAULT_CONFIG["sample_metadata_project"]
        self.sample_metadata_host = sample_metadata_host or DEFAULT_CONFIG["sample_metadata_host"]
        self.analysis_runner_project = analysis_runner_project or DEFAULT_CONFIG["analysis_runner_project"]
        self.analysis_runner_host = analysis_runner_host or DEFAULT_CONFIG["analysis_runner_host"]
        assert self.cloud in ("gcp", "azure"), f"Invalid cloud specification '{self.cloud}'"

    def to_dict(self) -> Dict[str, str]:
        return self.__dict__.copy()


def get_deploy_config() -> DeployConfig:
    global deploy_config
    if deploy_config is None:
        set_deploy_config_from_env()
    return deploy_config


def set_deploy_config(config: DeployConfig) -> None:
    global deploy_config
    logging.info(f"setting deploy_config: {json.dumps(config.__dict__)}")
    deploy_config = config


def set_deploy_config_from_env() -> None:
    set_deploy_config(DeployConfig.from_environment())


def get_server_config() -> Dict[str, Any]:
    global server_config
    if server_config is None:
        from .secrets import read_secret
        config_project = get_deploy_config().analysis_runner_project
        server_config = json.loads(read_secret(config_project, "server-config"))
    return server_config
