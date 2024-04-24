package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

var batchSize int = 1_000_000
var billion int = 1_000_000_000
var chunkSize int64 = 1_000_000
var inputFile string = "samples/measurements.txt"

type Details struct {
	sum   float32
	count int
	min   float32
	max   float32
}

type ReadTask struct {
	begin int64
	end   int64
}

func Min(x float32, y float32) float32 {
	if x < y {
		return x
	}
	return y
}
func Max(x float32, y float32) float32 {
	if x < y {
		return y
	}
	return x
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getFileSize() int64 {
	fi, err := os.Stat(inputFile)
	check(err)
	return fi.Size()
}

func fileReader2(c chan<- []string, in <-chan ReadTask) {
	file, err := os.Open(inputFile)
	check(err)
	defer file.Close()

	for task := range in {

		file.Seek(task.begin, 0)
		char := make([]byte, 1)
		if task.begin != 0 {
			for {
				file.Read(char)
				task.begin += 1
				if string(char) == "\n" {
					break
				}
			}
		}
		file.Seek(task.end, 0)
		for {
			file.Read(char)
			if string(char) == "\n" {
				break
			}
			task.end += 1
		}
		file.Seek(task.begin, 0)
		input := make([]byte, task.end-task.begin)
		file.Read(input)

		lines := strings.Split(string(input), "\n")

		c <- lines
	}
}

func fileReader(c chan<- []string, begin int, end int) {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	cnt := 0
	buffer := make([]string, batchSize)
	for scanner.Scan() {
		if cnt < begin {
			continue
		}
		if cnt > end {
			break
		}
		line := scanner.Text()
		if line == "" {
			break
		}
		buffer[cnt%batchSize] = line
		cnt += 1
		if cnt%batchSize == 0 {
			c <- buffer
			buffer = make([]string, batchSize)
		}
		if cnt%1000000 == 0 {
			fmt.Printf("Reader - %d/%d=%f\n", cnt, billion, float64((float32(cnt)/float32(billion))*100))
		}
	}
}

func processResults(in <-chan []string, out chan<- map[string]Details) {
	details := make(map[string]Details)
	for batch := range in {
		for _, line := range batch {

			splitIndex := strings.Index(line, ";")
			if splitIndex == -1 {
				fmt.Print(line)
				fmt.Print(" - Error here\n")
				continue
			}
			t, _ := strconv.ParseFloat(line[splitIndex+1:], 32)

			temp := float32(t)

			current, err := details[line[:splitIndex]]

			var sum float32
			var count int
			var min float32
			var max float32

			if !err {
				sum, count, min, max = temp, 1, temp, temp
			} else {
				sum = current.sum + temp
				count = current.count + 1
				min = Min(current.min, temp)
				max = Max(current.max, temp)
			}

			details[line[:splitIndex]] = Details{sum: sum, count: count, min: min, max: max}
		}
		out <- details
		details = make(map[string]Details)
	}
}

func aggregateOutput(in <-chan map[string]Details, done chan<- int) {

	details := make(map[string]Details)

	// cnt := 0

	for result_batch := range in {
		for station, detail := range result_batch {
			current, err := details[station]

			var sum float32
			var count int
			var min float32
			var max float32
			if !err {
				sum, count, min, max = detail.sum, detail.count, detail.min, detail.max
			} else {
				sum = current.sum + detail.sum
				count = current.count + detail.count
				min = Min(current.min, detail.min)
				max = Max(current.max, detail.max)
			}

			details[station] = Details{sum: sum, count: count, min: min, max: max}
		}
		// cnt += 1
		// if cnt%10 == 0 {
		// 	fmt.Printf("Aggregator batch - %d/%d=%f\n", cnt, billion/batchSize, float64((float32(cnt)/float32(billion/batchSize))*100))
		// }
	}
	// Print output
	output := "{"
	for station, d := range details {
		output += fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", station, d.min, d.sum/float32(d.count), d.max)
	}
	output = strings.TrimSuffix(output, ", ")
	output += "}\n"
	fmt.Print(output)
	done <- 0
}

func main() {
	// Start profiling
	f, err := os.Create("myprogram.prof")
	if err != nil {
		fmt.Println(err)
		return
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	start := time.Now()

	read_tasks := make(chan ReadTask, 1000000)
	file_input := make(chan []string, 100)
	result_output := make(chan map[string]Details, 100)
	done := make(chan int)

	var fileWg sync.WaitGroup

	fileSize := getFileSize()

	chunks := (fileSize + chunkSize - 1) / chunkSize

	for i := int64(0); i < chunks; i++ {
		read_tasks <- ReadTask{chunkSize * i, chunkSize * (i + 1)}
	}

	for i := 0; i < 10; i++ {
		fileWg.Add(1)
		go func() {
			defer fileWg.Done()
			fileReader2(file_input, read_tasks)
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processResults(file_input, result_output)
		}()
	}

	go aggregateOutput(result_output, done)

	close(read_tasks)

	fileWg.Wait()
	close(file_input)

	wg.Wait()
	close(result_output)

	<-done

	elapsed := time.Since(start)
	log.Printf("Took %s", elapsed)

}
