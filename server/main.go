package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/casey/govalent/server/api"
	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
)

// GET /executors
// POST /executors
// PUT /executors/{name}
//
// Executor interface
// POST /jobs {"executor_details": <executor details>, "tasks": <task group metadata>}
// DELETE /jobs/{job_id}
//
//

func main() {
	c := common.NewConfigFromEnv()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{AddSource: true, Level: c.LogLevel}))
	slog.SetDefault(logger)
	// log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)
	pool, err := db.GetDB(&c)
	if err != nil {
		slog.Error(fmt.Sprint("Error connecting to database: ", err))
	}
	slog.Info(fmt.Sprint("Connected to DB at ", c.Dsn))
	err = db.EmitDDL(pool)
	if err != nil {
		slog.Error(fmt.Sprint("Failed initialize db: ", err.Error()))
	}
	slog.Info(fmt.Sprint("Initialized DB at ", c.Dsn))
	s := api.NewGovalentAPIServer(&c, fmt.Sprintf(":%d", c.Port))
	s.AddRoutes(&c, pool)
	srv_err := s.Srv.ListenAndServe()
	slog.Error(srv_err.Error())
}
