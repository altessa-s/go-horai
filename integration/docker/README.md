# docker

Run `make test-integration-docker` from the root. Docker tests fail on unavailable infrastructure; they never silently skip.
Each contract receives an isolated database, key prefix, or table namespace. Containers are test-owned and terminated on cleanup.
