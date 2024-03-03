package read

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type weather struct {
	maxWeather float64
	minWeather float64
	sum        float64
	count      int64
}
type outputWeather struct {
	name       string
	maxWeather float64
	minWeather float64
	average    float64
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}

	return b
}

func roundTowardPositive(x float64) float64 {
	if x < 0 {
		return math.Floor(x*10+0.5) / 10
	}
	return math.Ceil(x*10-0.5) / 10
}

func minFloat(a, b float64) float64 {
	if a > b {
		return b
	}

	return a
}

func Worker(newTasks <-chan []byte, result map[string]*weather, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	for data := range newTasks {
		lines := strings.Split(string(data), "\n")

		for _, line := range lines {
			if len(line) == 0 {
				continue // Skip empty lines
			}
			lineStr := string(line)
			// Split the line into parts
			parts := strings.Split(lineStr, ";")
			if len(parts) != 2 {
				log.Printf("Invalid line: %s", lineStr)
				continue
			}

			num, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				log.Printf("Error parsing number: %v", err)
				continue
			}
			numRoundedToPositive := roundTowardPositive(num)

			mu.Lock()
			w, ok := result[parts[0]]
			if !ok {
				w = &weather{
					maxWeather: numRoundedToPositive,
					minWeather: numRoundedToPositive,
					sum:        num,
					count:      1,
				}
			} else {
				w.maxWeather = maxFloat(w.maxWeather, numRoundedToPositive)
				w.minWeather = minFloat(w.minWeather, numRoundedToPositive)
				w.sum += num
				w.count++
			}
			result[parts[0]] = w
			mu.Unlock()
		}
	}
}
func Read(fileDir string) error {
	file, err := os.Open(fileDir + "/weather_stations.csv")
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
	var mu sync.Mutex

	newTasks := make(chan []byte, 10)
	ResultCity := make(map[string]*weather)
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go Worker(newTasks, ResultCity, &wg, &mu)
	}

	reader := bufio.NewReader(file)
	buf := make([]byte, 0, 4*1024)
	var remainder []byte
	for {
		n, err := reader.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			continue
		}

		data := append(remainder, buf...)

		// Reset start for each new chunk of data.
		start := 0
		for i, b := range data {
			if b == '\n' {
				newTasks <- data[start:i]
				//process(data[start:i])
				start = i + 1
			}
		}

		remainder = data[start:]

	}

	close(newTasks)
	wg.Wait()

	allWeather := make([]outputWeather, len(ResultCity))
	idx := 0

	for key, value := range ResultCity {
		average := value.sum / float64(value.count)
		roundedAverage := roundTowardPositive(average)

		allWeather[idx] = outputWeather{
			maxWeather: value.maxWeather,
			minWeather: value.minWeather,
			average:    roundedAverage,
			name:       key,
		}

		idx++
	}

	sort.Slice(allWeather, func(i, j int) bool {
		return strings.Compare(allWeather[i].name, allWeather[j].name) == -1

	})

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

	for i, item := range allWeather {
		var line string
		if i == len(allWeather)-1 {
			line = fmt.Sprintf("%s=%.1f/%.1f/%.1f\n", item.name, item.maxWeather, item.minWeather, item.average)
		} else {
			line = fmt.Sprintf("%s=%.1f/%.1f/%.1f,\n", item.name, item.maxWeather, item.minWeather, item.average)
		}

		_, err = output.WriteString(line)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return err
		}
	}
	fmt.Println("Data has been written")

	return nil
}

//func process(chunk []byte) error {
//
//	lines := bytes.Split(chunk, []byte("\n"))
//
//	for _, line := range lines {
//		if len(line) == 0 {
//			continue // Skip empty lines
//		}
//		lineStr := string(line)
//		// Split the line into parts
//		parts := strings.Split(lineStr, ";")
//		if len(parts) != 2 {
//			return fmt.Errorf("Invalid line: %s", lineStr)
//		}
//
//		num, err := strconv.ParseFloat(parts[1], 64)
//		if err != nil {
//			return err
//		}
//		numRoundedToPositive := roundTowardPositive(num)
//		weatherValue := ResultCity[parts[0]]
//		weatherValue.maxWeather = maxFloat(ResultCity[parts[0]].maxWeather, numRoundedToPositive)
//		weatherValue.minWeather = maxFloat(ResultCity[parts[0]].minWeather, numRoundedToPositive)
//		weatherValue.sum += num
//		weatherValue.count++
//		ResultCity[parts[0]] = weatherValue
//	}
//	return nil
//}
