package main

// Optional Go profiler for performance analysis.
//
// OPTIMUSDB_PPROF=<host:port> (for example 0.0.0.0:6060) serves the standard Go profiling endpoints
// (/debug/pprof/: CPU, heap, goroutines, mutex and block contention) on that address. Unset (the default),
// nothing is started. The endpoints are served on their own listener, never on the API port; enable them only
// for measurements, on a trusted network.
//   OPTIMUSDB_PPROF_MUTEX  mutex contention sampling rate (default 5: one in five events; 0 = off)
//   OPTIMUSDB_PPROF_BLOCK  blocking-event sampling rate in nanoseconds (default 0 = off)

import (
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/ on http.DefaultServeMux (the API uses its own mux)
	"os"
	"runtime"
	"strconv"
)

func init() {
	addr := os.Getenv("OPTIMUSDB_PPROF")
	if addr == "" {
		return
	}
	rate := func(name string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
			return v
		}
		return def
	}
	runtime.SetMutexProfileFraction(rate("OPTIMUSDB_PPROF_MUTEX", 5))
	runtime.SetBlockProfileRate(rate("OPTIMUSDB_PPROF_BLOCK", 0))
	go func() {
		log.Printf("[PPROF] profiling endpoints on http://%s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, http.DefaultServeMux); err != nil {
			log.Printf("[PPROF] not started: %v", err)
		}
	}()
}
