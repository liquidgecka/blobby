package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	URL = flag.String(
		"url",
		"http://localhost:2000/test",
		"the URL to submit too.")
	MaxSize = flag.Int(
		"max_size",
		1024*1024*4,
		"The maximum amount of data to send in each iteration.")
	MinSize = flag.Int(
		"min_size",
		1024*16,
		"The minimum amount of data to send in each iteration.")
	Requests = flag.Int(
		"requests",
		1000000,
		"The total number of requests to make.")
	Parallel = flag.Int(
		"parallel",
		100,
		"The total number of parallel operations to run.")
)

var (
	data     []byte
	requests []request
	next     int32
	wg       sync.WaitGroup
	client   *http.Client
)

type request struct {
	size    int
	success bool
	time    time.Duration
}

type requestList []request

func (r requestList) Len() int {
	return len(r)
}

func (r requestList) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r requestList) Less(i, j int) bool {
	return r[i].time < r[j].time
}

func Inserter() {
	defer wg.Done()
	for {
		next := atomic.AddInt32(&next, 1)
		if next > int32(*Requests) {
			break
		}
		i := int(next - 1)
		requests[i].size = rand.Intn(*MaxSize-*MinSize) + *MinSize
		pass := data[0:requests[i].size]
		buffer := bytes.NewBuffer(pass)
		requests[i].size = len(pass)
		start := time.Now()
		req, _ := http.NewRequest("POST", *URL, buffer)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			requests[i].success = true
		} else {
			fmt.Printf("FAILED\n")
			if err != nil {
				fmt.Printf("Error %s\n", err.Error())
			}
			if resp != nil {
				body, _ := ioutil.ReadAll(resp.Body)
				fmt.Printf("Body: %s\n", string(body))
			}
		}
		if resp != nil && resp.Body != nil {
			ioutil.ReadAll(resp.Body)
		}
		requests[i].time = time.Now().Sub(start)
	}
}

func main() {
	flag.Parse()
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *Parallel,
			DisableCompression:  true,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	data = make([]byte, *MaxSize)
	rand.Read(data)
	requests = make([]request, *Requests)
	for i := 0; i < *Parallel; i++ {
		wg.Add(1)
		go Inserter()
	}
	wg.Wait()
	sort.Sort(requestList(requests))
	success := 0
	for _, r := range requests {
		if r.success {
			success += 1
		}
	}
	percent := func(f float32) int {
		x := int(float32(len(requests)) * (f / 100))
		return x
	}
	fmt.Printf("%d querues, %d failures\n", len(requests), len(requests)-success)
	fmt.Printf(" %%50   - %s\n", requests[percent(50.0)].time)
	fmt.Printf(" %%95   - %s\n", requests[percent(95.0)].time)
	fmt.Printf(" %%99   - %s\n", requests[percent(99.0)].time)
	fmt.Printf(" %%99.9 - %s\n", requests[percent(99.9)].time)
}
