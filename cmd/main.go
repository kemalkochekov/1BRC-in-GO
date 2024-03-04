package main

import (
	"1BRC_in_Go/internal/read"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	start := time.Now()
	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Could not set up environment variable: %s", err)
	}

	fileDir := os.Getenv("DATA_DIR")

	err := read.Read(fileDir)
	if err != nil {
		log.Printf("Failed to read the file %s", err.Error())
	}

	elapsed := time.Since(start)
	fmt.Printf("Execution time: %s\n", elapsed)
}
