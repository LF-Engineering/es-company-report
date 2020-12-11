package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

var (
	gESURL string
)

func fatalError(err error) {
	if err == nil {
		return
	}
	fmt.Printf("error: %v\n", err)
	panic("")
}

func fatal(str string) {
	fmt.Printf("error: %s\n", str)
	panic("")
}

func getIndices(res map[string]interface{}) (indices []string) {
	ary, _ := res["x"].([]interface{})
	for _, i := range ary {
		item, _ := i.(map[string]interface{})
		idx, _ := item["index"].(string)
		if !strings.HasPrefix(idx, "sds-") {
			continue
		}
		if strings.HasSuffix(idx, "-raw") || strings.HasSuffix(idx, "-for-merge") {
			continue
		}
		indices = append(indices, idx)
	}
	sort.Strings(indices)
	return
}

func getRoots(indices, aliases []string) (roots []string) {
	fmt.Printf("%d indices, %d aliases\n", len(indices), len(aliases))
	return
}

func getSlugRoots() (slugRoots []string) {
	method := "GET"
	url := gESURL + "/_cat/indices?format=json"
	req, err := http.NewRequest(method, url, nil)
	fatalError(err)
	resp, err := http.DefaultClient.Do(req)
	fatalError(err)
	body, err := ioutil.ReadAll(resp.Body)
	fatalError(err)
	_ = resp.Body.Close()
	body = append([]byte(`{"x":`), body...)
	body = append(body, []byte(`}`)...)
	var result map[string]interface{}
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	indices := getIndices(result)
	url = gESURL + "/_cat/aliases?format=json"
	req, err = http.NewRequest(method, url, nil)
	fatalError(err)
	resp, err = http.DefaultClient.Do(req)
	fatalError(err)
	body, err = ioutil.ReadAll(resp.Body)
	fatalError(err)
	_ = resp.Body.Close()
	body = append([]byte(`{"x":`), body...)
	body = append(body, []byte(`}`)...)
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	aliases := getIndices(result)
	slugRoots = getRoots(indices, aliases)
	return
}

func main() {
	gESURL = os.Getenv("ES_URL")
	if gESURL == "" {
		fatal("ES_URL must be set")
	}
	slugRoots := getSlugRoots()
	fmt.Printf("slugRoots: %v\n", slugRoots)
}
