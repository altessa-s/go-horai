module github.com/altessa-s/go-horai/storage/mongo

go 1.27.0

toolchain go1.27.3

require (
	github.com/altessa-s/go-horai v0.1.0
	github.com/stretchr/testify v1.12.1
	go.mongodb.org/mongo-driver/v2 v2.8.0
)

require (
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/altessa-s/go-horai => ../..
