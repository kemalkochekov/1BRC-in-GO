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

func convertStringToInt64(input string) int64 {
	isNegative := false
	if input[0] == '-' {
		isNegative = true
		input = input[1:]
	}
	var num int64
	if len(input) == 3 {
		num = int64(input[0])*10 + int64(input[2]) - int64('0')*11
	} else {
		num = int64(input[0])*100 + int64(input[1])*10 + int64(input[3]) - (int64('0') * 111)
	}
	if isNegative {
		return -num
	}

	return num
}
func roundFloat(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}

// Choose an appropriate number of shards.
const (
	bufferedChannels = 15
	chunkSize        = 64 * 1024 * 1024
)

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

		num := convertStringToInt64(parts[1])

		if val, ok := mapSend[parts[0]]; ok {
			if val.maxWeather < num {
				val.maxWeather = num
			}
			if val.minWeather > num {
				val.minWeather = num
			}
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

	for w := 0; w < numWorkers-1; w++ {
		wg.Add(1)
		go func() {
			for data := range newTasks {
				processData(data, mapStream)
			}
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
				if val.maxWeather < info.maxWeather {
					val.maxWeather = info.maxWeather
				}
				if val.minWeather > info.minWeather {
					val.minWeather = info.minWeather
				}
				merge[city] = val
			} else {
				merge[city] = info
			}
		}
	}

	return WriteOutput(merge, fileDir)
}
func WriteOutput(data map[string]weather, fileDir string) error {

	allWeather := make([]outputWeather, len(data))
	idx := 0

	for key, value := range data {
		allWeather[idx] = outputWeather{
			maxWeather: roundFloat(float64(value.maxWeather) / 10.0),
			minWeather: roundFloat(float64(value.minWeather) / 10.0),
			average:    roundFloat(float64(value.sum) / 10.0 / float64(value.count)),
			name:       key,
		}
		fmt.Println(allWeather[idx].maxWeather)
		idx++
	}

	sort.Slice(allWeather, func(i, j int) bool {
		return allWeather[i].name < allWeather[j].name
	})

	var resultStringBuilder strings.Builder
	for _, item := range allWeather {
		resultStringBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", item.name, item.minWeather, item.average, item.maxWeather))
	}

	file, err := os.Create(fileDir + "/output.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(resultStringBuilder.String())
	if err != nil {
		return err
	}

	return nil
}
