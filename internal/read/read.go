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

// Choose an appropriate number of shards.
const (
	batchSize        = 1000
	bufferedChannels = 100
	chunkSize        = 64 * 1024 * 1024
)

func Worker(newTasks <-chan []byte, mapStream chan<- map[string]weather) {
	for data := range newTasks {
		processData(data, mapStream)
	}
}

func processData(data []byte, mapStream chan<- map[string]weather) {
	lines := strings.Split(string(data), "\n")

	mapSend := make(map[string]weather)
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

		if val, ok := mapSend[parts[0]]; ok {
			val.maxWeather = maxInt(val.maxWeather, num)
			val.minWeather = minInt(val.minWeather, num)
			val.sum += num
			val.count++
			mapSend[parts[0]] = val
		} else {
			mapSend[parts[0]] = weather{
				maxWeather: num,
				minWeather: num,
				sum:        num,
				count:      1,
			}
		}
	}
	mapStream <- mapSend
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

	mapStream := make(chan map[string]weather, 10)
	newTasks := make(chan []byte, bufferedChannels)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			Worker(newTasks, mapStream)
			wg.Done()
		}()
	}

	go func() {

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
		close(newTasks)
		wg.Wait()
		close(mapStream)
	}()

	merge := make(map[string]weather)
	for mapOne := range mapStream {
		for city, info := range mapOne {
			if val, ok := merge[city]; ok {
				val.sum += info.sum
				val.count += info.count
				val.maxWeather = maxInt(val.maxWeather, info.maxWeather)
				val.minWeather = minInt(val.minWeather, info.minWeather)
				merge[city] = val
			} else {
				merge[city] = info
			}
		}
	}

	return WriteOutput(fileDir, merge)
}
func WriteOutput(fileDir string, data map[string]weather) error {
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
