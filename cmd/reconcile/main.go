package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prateek-pradhan/logsense/pkg/storage"
)

func main() {
	idsFile := flag.String("ids-file", "", "file of expected event IDs (one per line)")
	mongoURI := flag.String("mongo", "mongodb://localhost:27017", "mongo connection URI")

	flag.Parse()

	if *idsFile == "" {
		log.Fatal("--ids-file is required")
	}

	expected, dupLines := readIDs(*idsFile)
	fmt.Printf("Expected: %d unique IDs (%d duplicate lines in file)\n", len(expected), dupLines)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := storage.Connect(ctx, *mongoURI)
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}

	defer store.Close(ctx)

	found, err := store.ExistingIDs(ctx, expected)
	if err != nil {
		log.Fatalf("lookup: %v", err)
	}

	var missing []string
	for _, id := range expected {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}

	fmt.Printf("found in mongo: %d / %d\n", len(found), len(expected))
	if len(missing) == 0 {
		fmt.Println("RESULT: PASS - no loss, no duplicates (every produced ID present exactly once)")
		return
	}

	fmt.Printf("RESULT: FAIL - %d events MISSING from mongo\n", len(missing))

	for i, id := range missing {
		if i >= 5 {
			fmt.Printf(" ... and %d more\n", len(missing)-5)
			break
		}
		fmt.Printf(" missing: %s\n", id)
	}
	os.Exit(1)
}

func readIDs(path string) (unique []string, dupLines int) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open ids-file: %v", err)
	}
	defer f.Close()

	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		id := scanner.Text()
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			dupLines++
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("read ids-file: %v", err)
	}
	return unique, dupLines
}
