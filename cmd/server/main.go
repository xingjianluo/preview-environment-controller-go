package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/controller"
	"github.com/xingjianluo/preview-environment-controller-go/internal/httpapi"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	maxEnvs := flag.Int("max-envs", 8, "maximum number of environments that count against quota")
	portLow := flag.Int("port-low", 19000, "lowest allocated host port")
	portHigh := flag.Int("port-high", 19100, "highest allocated host port")
	ttl := flag.Duration("ttl", 60*time.Second, "default environment TTL")
	reapEvery := flag.Duration("reap-every", 5*time.Second, "expired-environment reaper interval")
	flag.Parse()

	c := controller.New(controller.Config{
		MaxEnvironments: *maxEnvs,
		PortLow:         *portLow,
		PortHigh:        *portHigh,
		DefaultTTL:      *ttl,
		Provisioner:     controller.NewSimProvisioner(),
	})
	defer c.Close()

	reaper := controller.NewReaper(c, *reapEvery)
	reaper.Start()
	defer reaper.Stop()

	mux := http.NewServeMux()
	httpapi.NewHandler(c).Register(mux)
	return http.ListenAndServe(*addr, mux)
}
