package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Details struct {
	sum   float32
	count int
	min   float32
	max   float32
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

func main() {
	start := time.Now()

	file, err := os.Open("samples/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	details := make(map[string]Details)

	cnt := 0

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}
		d := strings.Split(line, ";")
		station := d[0]
		t, _ := strconv.ParseFloat(d[1], 32)

		temp := float32(t)

		current, err := details[station]

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

		details[station] = Details{sum: sum, count: count, min: min, max: max}

		cnt += 1
		if cnt%1000000 == 0 {
			fmt.Printf("%d/%d=%f\n", cnt, 1000000000, float64((float32(cnt)/float32(1000000000))*100))
		}

	}
	output := "{"
	for station, d := range details {
		output += fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", station, d.min, d.sum/float32(d.count), d.max)
	}
	output = strings.TrimSuffix(output, ", ")
	output += "}"
	fmt.Print(output)

	elapsed := time.Since(start)
	log.Printf("Took %s", elapsed)
}
