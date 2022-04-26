import pytest
from cpg_utils.auth import get_user_from_headers
from cpg_utils.config import DeployConfig, set_deploy_config

# Mocked tokens from https://www.javainuse.com/jwtgenerator
TEST_TOKEN1 = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1MDczNDA0NSwiaWF0IjoxNjUwNzM0MDQ1LCJlbWFpbCI6InRlc3QxQHRlc3QuY29tIn0.OJ-39xdDbIH8FDsdlwFsIwyDzgSbA_gOtYbRNhBmLxo"
TEST_TOKEN2 = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1MDczNDA0NSwiaWF0IjoxNjUwNzM0MDQ1LCJlbWFpbCI6InRlc3QyQHRlc3QuY29tIn0.ONbKZ4cf0jb9wtVJBprdtRhAhc5KVp9hSAPWN6ukt9A"
TEST_TOKEN3 = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1MDczNDA0NSwiaWF0IjoxNjUwNzM0MDQ1LCJlbWFpbCI6InRlc3QzQHRlc3QuY29tIn0.njCokays7b_Yl2O0_1lKROvLV-MiA0RW4bwx68dqeTo"


def test_bogus_header(monkeypatch):
    assert get_user_from_headers({}) is None
    headers = { "x-goog-iap-jwt-assertion" : "bogus" }
    monkeypatch.setenv("CLOUD", "gcp")
    set_deploy_config(DeployConfig.from_environment())
    with pytest.raises(ValueError, match="Wrong number of segments in token"):
        get_user_from_headers(headers)
    headers = { "Authorization" : "Bearer bogus" }
    with pytest.raises(ValueError, match="Wrong number of segments in token"):
        get_user_from_headers(headers)


def test_headers(monkeypatch):
    headers = {
        "x-goog-iap-jwt-assertion" : TEST_TOKEN1,
        "x-ms-client-principal-name" : "test2@test.com",
        "Authorization" : "Bearer " + TEST_TOKEN3
    }
    monkeypatch.setenv("CLOUD", "gcp")
    set_deploy_config(DeployConfig.from_environment())
    assert get_user_from_headers(headers) == "test1@test.com"
    monkeypatch.setenv("CLOUD", "azure")
    set_deploy_config(DeployConfig.from_environment())
    assert get_user_from_headers(headers) == "test2@test.com"
    headers = {
        "x-goog-iap-jwt-assertion" : TEST_TOKEN1,
        "Authorization" : "Bearer " + TEST_TOKEN3
    }
    assert get_user_from_headers(headers) == "test3@test.com"
