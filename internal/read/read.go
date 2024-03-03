package read

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type weather struct {
	maxWeather int64
	minWeather int64
	sum        int64
	count      int64
}
type outputWeather struct {
	name       string
	maxWeather float64
	minWeather float64
	average    float64
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

func convertStringToInt64(input string) (int64, error) {
	input = input[:len(input)-2] + input[len(input)-1:]
	output, err := strconv.ParseInt(input, 10, 64)
	return output, err
}
func roundFloat(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}

func minInt(a, b int64) int64 {
	if a > b {
		return b
	}

	return a
}

type Shard struct {
	mu     sync.Mutex
	result map[string]*weather
}

func NewShard() *Shard {
	return &Shard{
		result: make(map[string]*weather),
	}
}

// Choose an appropriate number of shards.
const (
	numShards        = 32
	batchSize        = 1000
	bufferedChannels = 100
	chunkSize        = 64 * 1024 * 1024
)

var shards [numShards]*Shard

func init() {
	for i := 0; i < numShards; i++ {
		shards[i] = NewShard()
	}
}

func getShard(key string) *Shard {
	// Simple hash function to choose a shard.
	return shards[uint(fnv32(key))%numShards]
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

func Worker(newTasks <-chan []byte) {
	for data := range newTasks {
		processData(data)
	}
}

func processData(data []byte) {
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue // Skip empty lines
		}
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			log.Printf("Invalid line: %s", line)
			continue
		}

		num, err := convertStringToInt64(parts[1])
		if err != nil {
			log.Printf("Error converting string to int64: %v", err)
			continue
		}

		shard := getShard(parts[0])
		shard.mu.Lock()
		w, ok := shard.result[parts[0]]
		if !ok {
			w = &weather{
				maxWeather: num,
				minWeather: num,
				sum:        num,
				count:      1,
			}
		} else {
			w.maxWeather = maxInt(w.maxWeather, num)
			w.minWeather = minInt(w.minWeather, num)
			w.sum += num
			w.count++
		}
		shard.result[parts[0]] = w
		shard.mu.Unlock()
	}
}

func MergeResults() map[string]*weather {
	merged := make(map[string]*weather)

	for _, shard := range shards {
		shard.mu.Lock()
		for key, value := range shard.result {
			if _, ok := merged[key]; !ok {
				merged[key] = &weather{
					maxWeather: value.maxWeather,
					minWeather: value.minWeather,
					sum:        value.sum,
					count:      value.count,
				}
			} else {
				merged[key].maxWeather = maxInt(merged[key].maxWeather, value.maxWeather)
				merged[key].minWeather = minInt(merged[key].minWeather, value.minWeather)
				merged[key].sum += value.sum
				merged[key].count += value.count
			}
		}
		shard.mu.Unlock()
	}

	return merged
}

func Read(fileDir string) error {
	file, err := os.Open(fileDir + "/measurements.txt")
	if err != nil {
		log.Printf("Open: %s", err.Error())
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Printf("Failed to close %s", err.Error())
		}
	}()

	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	newTasks := make(chan []byte, bufferedChannels)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			Worker(newTasks)
			wg.Done()
		}()
	}

	go func() {
		defer close(newTasks)
		buf := make([]byte, chunkSize)
		remainder := make([]byte, 0, chunkSize)
		for {
			n, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Println("Error in reading")
			}
			buf = buf[:n]

			Onechunk := make([]byte, n)
			copy(Onechunk, buf)

			lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})

			Onechunk = append(remainder, buf[:lastNewLineIndex+1]...)
			remainder = make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(remainder, buf[lastNewLineIndex+1:])

			newTasks <- Onechunk
		}
	}()

	wg.Wait()

	merged := MergeResults()

	return WriteOutput(fileDir, merged)
}
func WriteOutput(fileDir string, data map[string]*weather) error {
	output, err := os.OpenFile(fileDir+"/output.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("Failed to Create file or write file %w", err)
	}

	defer func() {
		err = output.Close()
		if err != nil {
			log.Printf("Failed to close %s", err.Error())
		}
	}()

	allWeather := make([]outputWeather, len(data))
	idx := 0

	for key, value := range data {
		average := roundFloat(float64(value.sum) / 10.0 / float64(value.count))

		allWeather[idx] = outputWeather{
			maxWeather: roundFloat(float64(value.maxWeather) / 10.0),
			minWeather: roundFloat(float64(value.minWeather) / 10.0),
			average:    average,
			name:       key,
		}

		idx++
	}

	sort.Slice(allWeather, func(i, j int) bool {
		return allWeather[i].name < allWeather[j].name
	})

	var outputLines []string
	for _, item := range allWeather {
		line := fmt.Sprintf("%s=%.1f/%.1f/%.1f\n", item.name, item.maxWeather, item.minWeather, item.average)
		outputLines = append(outputLines, line)
		if len(outputLines) >= batchSize {
			writeBatchToFile(output, outputLines)
			outputLines = outputLines[:0]
		}
	}
	fmt.Println("Data has been written")
	if len(outputLines) > 0 {
		writeBatchToFile(output, outputLines)
	}

	return nil
}

func writeBatchToFile(file *os.File, lines []string) {
	for _, line := range lines {
		_, err := file.WriteString(line)
		if err != nil {
			log.Fatalf("Failed to write batch to file: %v", err)
		}
	}
}
