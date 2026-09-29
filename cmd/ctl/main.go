package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
	"github.com/xingjianluo/preview-environment-controller-go/internal/httpapi"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("missing subcommand")
	}

	// The subcommand is the first token that names a known command, so global
	// flags and their values may be written either before or after it.
	known := map[string]bool{
		"apply": true, "occupy": true, "release": true,
		"release-all": true, "reclaim": true, "reap-expired": true,
		"list": true, "get": true,
	}
	cmdIdx := -1
	for i, a := range args {
		if known[a] {
			cmdIdx = i
			break
		}
	}
	if cmdIdx < 0 {
		usage()
		return fmt.Errorf("missing or unknown subcommand")
	}
	cmd := args[cmdIdx]
	rest := append(append([]string{}, args[:cmdIdx]...), args[cmdIdx+1:]...)

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	server := fs.String("server", envOr("PEC_ADDR", "http://127.0.0.1:8080"), "controller base URL")
	name := fs.String("name", "", "environment name")
	target := fs.String("target", "", "application target (e.g. PR/branch ref)")
	holder := fs.String("holder", "", "occupant identifier")
	ttl := fs.Duration("ttl", 0, "time-to-live (e.g. 30s, 5m)")
	reqID := fs.String("request-id", "", "idempotency/diagnostic request identifier")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	positional := fs.Args()
	if *name == "" && len(positional) > 0 && cmd != "apply" && cmd != "list" && cmd != "reap-expired" {
		*name = positional[0]
	}
	if *target == "" && len(positional) > 0 && cmd == "apply" {
		*target = positional[0]
	}

	client := httpapi.NewClient(*server)
	ctx := context.Background()

	switch cmd {
	case "apply":
		if *target == "" {
			return fmt.Errorf("apply requires -target")
		}
		env, err := client.Apply(ctx, httpapi.ApplyParams{
			Name: *name, Target: *target, Holder: *holder,
			TTL: *ttl, RequestID: orGenerate(*reqID),
		})
		if err != nil {
			return err
		}
		return printEnv(env)
	case "occupy":
		return action(cmd, func() (*environment.Environment, error) {
			return client.Occupy(ctx, *name, *holder, orGenerate(*reqID))
		})
	case "release":
		return action(cmd, func() (*environment.Environment, error) {
			return client.Release(ctx, *name, *holder, orGenerate(*reqID))
		})
	case "release-all":
		return action(cmd, func() (*environment.Environment, error) {
			return client.ReleaseAll(ctx, *name, orGenerate(*reqID))
		})
	case "reclaim":
		return action(cmd, func() (*environment.Environment, error) {
			return client.Reclaim(ctx, *name, orGenerate(*reqID))
		})
	case "reap-expired":
		if err := client.ReclaimExpired(ctx); err != nil {
			return err
		}
		fmt.Println("sweep completed")
		return nil
	case "list":
		envs, err := client.List(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%-28s %-20s %-10s %-6s %-8s %s\n", "NAME", "TARGET", "STATUS", "PORT", "HOLDERS", "EXPIRES_AT")
		for _, e := range envs {
			fmt.Printf("%-28s %-20s %-10s %-6d %-8d %s\n",
				e.Name, e.Target, e.Status, e.Port, len(e.Holders),
				e.ExpiresAt.Format(time.RFC3339))
		}
		return nil
	case "get":
		env, err := client.Get(ctx, *name)
		if err != nil {
			return err
		}
		return printEnv(env)
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func action(cmd string, fn func() (*environment.Environment, error)) error {
	env, err := fn()
	if err != nil {
		return err
	}
	return printEnv(env)
}

func printEnv(e *environment.Environment) error {
	raw, err := json.MarshalIndent(struct {
		Name         string                       `json:"name"`
		Target       string                       `json:"target"`
		Status       environment.Status           `json:"status"`
		Port         int                          `json:"port"`
		PID          int                          `json:"pid"`
		Holders      []string                     `json:"holders"`
		Endpoint     string                       `json:"endpoint"`
		TTL          string                       `json:"ttl"`
		ExpiresAt    time.Time                    `json:"expires_at"`
		ClaimVersion int64                        `json:"claim_version"`
		Events       []environment.Event          `json:"events"`
		ReclaimErrs  []environment.ReclaimFailure `json:"reclaim_errors,omitempty"`
	}{
		Name: e.Name, Target: e.Target, Status: e.Status, Port: e.Port,
		PID: e.PID, Holders: e.Holders, Endpoint: e.Endpoint,
		TTL: e.TTL.String(), ExpiresAt: e.ExpiresAt,
		ClaimVersion: e.ClaimVersion, Events: e.Events, ReclaimErrs: e.ReclaimErrs,
	}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(raw))
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  ctl apply -target T [-name N] [-holder H] [-ttl D] [-request-id ID]
  ctl occupy -name N -holder H [-request-id ID]
  ctl release -name N -holder H [-request-id ID]
  ctl release-all -name N [-request-id ID]
  ctl reclaim -name N [-request-id ID]
  ctl reap-expired
  ctl list
  ctl get -name N`)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func orGenerate(id string) string {
	if id != "" {
		return id
	}
	return fmt.Sprintf("cli-%d", time.Now().UnixNano())
}
