module github.com/altessa-s/go-horai/storage/redis

go 1.27.0

toolchain go1.27.3

require (
	github.com/altessa-s/go-atlas-core v0.3.0
	github.com/altessa-s/go-horai v0.0.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

replace github.com/altessa-s/go-horai => ../..
