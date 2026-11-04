// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"os"
	"testing"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/storagetest"
)

func BenchmarkStores(b *testing.B) {
	for _, kind := range []string{"redis", "mongo", "postgres", "mysql"} {
		b.Run(kind, func(b *testing.B) {
			var addr string

			switch kind {
			case "redis":
				addr = os.Getenv("HORAI_REDIS_ADDR")
				if addr == "" {
					addr = endpoint(b, "redis/redis-stack-server:7.4.0-v3", "6379/tcp", nil)
				}
			case "mongo":
				addr = os.Getenv("HORAI_MONGO_URI")
				if addr == "" {
					addr = "mongodb://" + endpoint(b, "mongo:8", "27017/tcp", nil)
				}
			case "postgres":
				addr = os.Getenv("HORAI_POSTGRES_DSN")
				if addr == "" {
					addr = "postgres://postgres:horai-test@" + endpoint(b, "postgres:17", "5432/tcp", map[string]string{"POSTGRES_PASSWORD": "horai-test", "POSTGRES_DB": "horai"}) + "/horai?sslmode=disable"
				}
			case "mysql":
				addr = os.Getenv("HORAI_MYSQL_DSN")
				if addr == "" {
					addr = "root:horai-test@tcp(" + endpoint(b, "mysql:8.4", "3306/tcp", map[string]string{"MYSQL_ROOT_PASSWORD": "horai-test", "MYSQL_DATABASE": "horai"}) + ")/horai"
				}
			}

			storagetest.Bench(b, func(t testing.TB) horai.Store { t.Helper(); return openStore(t, kind, addr, namespace()) })
		})
	}
}
