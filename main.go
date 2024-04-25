package main

import (
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
var chunkSize int64 = 1
var inputFile = "samples/measurements-10.txt"

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

type TaskWorker struct {
	station string
	detail  Details
}

func GetTaskWorker(station string, input <-chan string, result chan<- TaskWorker) {
	var details Details
	init := false
	for row := range input {
		t, _ := strconv.ParseFloat(row, 32)
		temp := float32(t)
		if !init {
			details.count = 1
			details.sum = temp
			details.min = temp
			details.max = temp
		} else {
			details.sum = details.sum + temp
			details.count = details.count + 1
			details.min = Min(details.min, temp)
			details.max = Max(details.max, temp)
		}
	}
	result <- TaskWorker{
		station: station,
		detail:  details,
	}
}

func CreateTaskWorker(station string, tempString string) TaskWorker {
	t, _ := strconv.ParseFloat(tempString, 32)
	temp := float32(t)
	return TaskWorker{
		station: station,
		detail: Details{
			sum:   temp,
			count: 1,
			min:   temp,
			max:   temp,
		},
	}
}

func ProcessRow(worker *TaskWorker, tempString string) {
	t, _ := strconv.ParseFloat(tempString, 32)
	temp := float32(t)

	worker.detail.sum = worker.detail.sum + temp
	worker.detail.count = worker.detail.count + 1
	worker.detail.min = Min(worker.detail.min, temp)
	worker.detail.max = Max(worker.detail.max, temp)
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

func fileReader(c chan<- []string, in <-chan ReadTask) {
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
		if task.end-task.begin < 0 {
			return
		}
		input := make([]byte, task.end-task.begin)
		file.Read(input)

		lines := strings.Split(string(input), "\n")

		c <- lines
	}
}

func processResults(in <-chan []string, result chan<- TaskWorker) {
	workerMap := make(map[string]chan string)
	results := make(chan TaskWorker)
	for batch := range in {
		for _, line := range batch {

			splitIndex := strings.Index(line, ";")
			if splitIndex == -1 {
				fmt.Print(line)
				fmt.Print(" - Error here\n")
				continue
			}

			station := line[:splitIndex]

			worker, ok := workerMap[station]
			if !ok {
				input := make(chan string)
				go GetTaskWorker(station, input, results)
				input <- line[splitIndex+1:]
				workerMap[station] = input
			} else {
				worker <- line[splitIndex+1:]
			}
		}
	}
	for _, c := range workerMap {
		close(c)
	}
	for r := range results {
		result <- r
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
	resultChan := make(chan TaskWorker, 10000)

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
			fileReader(file_input, read_tasks)
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processResults(file_input, resultChan)
		}()
	}

	close(read_tasks)

	fileWg.Wait()
	close(file_input)

	output := "{"
	for results := range resultChan {
		output += fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", results.station, results.detail.min, results.detail.sum/float32(results.detail.count), results.detail.max)
	}
	output = strings.TrimSuffix(output, ", ")
	output += "}\n"
	fmt.Print(output)

	elapsed := time.Since(start)
	log.Printf("Took %s", elapsed)

}
