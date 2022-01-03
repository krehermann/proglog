package main

import (
	"log"

	"github.com/krehermann/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8099")
	log.Fatal(srv.ListenAndServe())
}
