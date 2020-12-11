package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	jsoniter "github.com/json-iterator/go"

	_ "github.com/go-sql-driver/mysql"
)

var (
	gESURL string
	gDB    *sqlx.DB
	gOrg   string
)

// contributor name, email address, project slug and affiliation date range
type contribReportItem struct {
	uuid    string
	name    string
	email   string
	project string
	n       int
	from    time.Time
	to      time.Time
}
type contribReport struct {
	items   []contribReportItem
	summary map[string]contribReportItem
}

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
	dss := make(map[string]struct{})
	all := make(map[string]struct{})
	for _, idx := range indices {
		ary := strings.Split(idx, "-")
		lAry := len(ary)
		dss[ary[lAry-1]] = struct{}{}
		root := strings.Join(ary[1:lAry-1], "-")
		if strings.HasSuffix(root, "-github") {
			root = root[:len(root)-7]
		}
		if root == "" {
			continue
		}
		all[root] = struct{}{}
	}
	for _, idx := range aliases {
		ary := strings.Split(idx, "-")
		lAry := len(ary)
		dss[ary[lAry-1]] = struct{}{}
		root := strings.Join(ary[1:lAry-1], "-")
		if strings.HasSuffix(root, "-github") {
			root = root[:len(root)-7]
		}
		if root == "" {
			continue
		}
		all[root] = struct{}{}
	}
	fmt.Printf("%d data source types\n", len(dss))
	for root := range all {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	fmt.Printf("%d projects detected\n", len(roots))
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

func jsonEscape(str string) string {
	b, _ := jsoniter.Marshal(str)
	return string(b[1 : len(b)-1])
}

func timeParseES(dtStr string) (time.Time, error) {
	dtStr = strings.TrimSpace(strings.Replace(dtStr, "Z", "", -1))
	ary := strings.Split(dtStr, "+")
	ary2 := strings.Split(ary[0], ".")
	var s string
	if len(ary2) == 1 {
		s = ary2[0] + ".000"
	} else {
		if len(ary2[1]) > 3 {
			ary2[1] = ary2[1][:3]
		}
		s = strings.Join(ary2, ".")
	}
	return time.Parse("2006-01-02T15:04:05.000", s)
}

func toMDYDate(dt time.Time) string {
	return fmt.Sprintf("%d/%d/%d", dt.Month(), dt.Day(), dt.Year())
}

func reportForRoot(ch chan []contribReportItem, root string) (items []contribReportItem) {
	defer func() {
		ch <- items
	}()
	// fmt.Printf("running for: %s\n", root)
	pattern := jsonEscape("sds-" + root + "-*,-*-raw,-*-for-merge")
	org := jsonEscape(gOrg)
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select author_uuid, count(*) as cnt, min(metadata__updated_on) as f, max(metadata__updated_on) as t from \"%s\" where author_org_name = '%s' group by author_uuid","fetch_size":%d}`,
		pattern,
		org,
		10000,
	)
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	url := gESURL + "/_sql?format=json"
	req, err := http.NewRequest(method, url, payloadBody)
	fatalError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	fatalError(err)
	body, err := ioutil.ReadAll(resp.Body)
	fatalError(err)
	_ = resp.Body.Close()
	type resultType struct {
		Cursor string          `json:"cursor"`
		Rows   [][]interface{} `json:"rows"`
	}
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if len(result.Rows) == 0 {
		return
	}
	processResults := func() {
		for _, row := range result.Rows {
			// [uuidstr 6 2019-06-25T06:07:45.000Z 2019-07-05T23:17:20.000Z]
			uuid, _ := row[0].(string)
			fN, _ := row[1].(float64)
			n := int(fN)
			from, _ := timeParseES(row[2].(string))
			to, _ := timeParseES(row[3].(string))
			item := contribReportItem{
				uuid:    uuid,
				n:       n,
				from:    from,
				to:      to,
				project: root,
			}
			items = append(items, item)
		}
	}
	processResults()
	for {
		data = `{"cursor":"` + result.Cursor + `"}`
		payloadBytes = []byte(data)
		payloadBody = bytes.NewReader(payloadBytes)
		req, err = http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		fatalError(err)
		body, err = ioutil.ReadAll(resp.Body)
		fatalError(err)
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if len(result.Rows) == 0 {
			break
		}
		processResults()
	}
	url = gESURL + "/_sql/close"
	data = `{"cursor":"` + result.Cursor + `"}`
	payloadBytes = []byte(data)
	payloadBody = bytes.NewReader(payloadBytes)
	req, err = http.NewRequest(method, url, payloadBody)
	fatalError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	fatalError(err)
	body, err = ioutil.ReadAll(resp.Body)
	fatalError(err)
	_ = resp.Body.Close()
	// fmt.Printf("%s: %v\n", root, items)
	return
}

func enrichReport(report *contribReport) {
	uuids := make(map[string][]string)
	for _, item := range report.items {
		uuids[item.uuid] = []string{}
	}
	nUUIDs := len(uuids)
	if nUUIDs == 0 {
		return
	}
	uuidsAry := []string{}
	for uuid := range uuids {
		uuidsAry = append(uuidsAry, uuid)
	}
	packSize := 1000
	nPacks := nUUIDs / packSize
	if nUUIDs%packSize != 0 {
		nPacks++
	}
	for i := 0; i < nPacks; i++ {
		from := i * packSize
		to := from + packSize
		if to > nUUIDs {
			to = nUUIDs
		}
		args := []interface{}{}
		query := "select uuid, name, email from profiles where uuid in("
		for _, uuid := range uuidsAry[from:to] {
			query += "?,"
			args = append(args, uuid)
		}
		query = query[:len(query)-1] + ")"
		rows, err := gDB.Query(query, args...)
		fatalError(err)
		var (
			uuid   string
			pName  *string
			pEmail *string
		)
		for rows.Next() {
			err = rows.Scan(&uuid, &pName, &pEmail)
			fatalError(err)
			name, email := "", ""
			if pName != nil {
				name = *pName
			}
			if pEmail != nil {
				email = *pEmail
			}
			uuids[uuid] = []string{name, email}
		}
		fatalError(rows.Err())
		fatalError(rows.Close())
	}
	for i, item := range report.items {
		data, ok := uuids[item.uuid]
		if !ok || (ok && len(data) == 0) {
			fmt.Printf("uuid %s not found in the database\n", item.uuid)
			continue
		}
		report.items[i].name = data[0]
		report.items[i].email = data[1]
	}
}

func summaryReport(report *contribReport) {
	report.summary = make(map[string]contribReportItem)
	for _, item := range report.items {
		uuid := item.uuid
		sItem, ok := report.summary[uuid]
		if !ok {
			sItem = item
			sItem.project = ""
			report.summary[uuid] = sItem
			continue
		}
		sItem.n += item.n
		if sItem.from.After(item.from) {
			sItem.from = item.from
		}
		if sItem.to.Before(item.to) {
			sItem.to = item.to
		}
		report.summary[uuid] = sItem
	}
}

func saveReport(report []contribReportItem) {
	rows := [][]string{}
	for _, item := range report {
		row := []string{item.name, item.email, item.project, strconv.Itoa(item.n), toMDYDate(item.from), toMDYDate(item.to), item.uuid}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			iN, _ := strconv.Atoi(rows[i][3])
			jN, _ := strconv.Atoi(rows[j][3])
			if iN == jN {
				return rows[i][0] > rows[j][0]
			}
			return iN > jN
		},
	)
	var writer *csv.Writer
	file, err := os.Create("report.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "project", "contributions", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
}

func saveSummaryReport(report map[string]contribReportItem) {
	rows := [][]string{}
	for _, item := range report {
		row := []string{item.name, item.email, strconv.Itoa(item.n), toMDYDate(item.from), toMDYDate(item.to), item.uuid}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			iN, _ := strconv.Atoi(rows[i][2])
			jN, _ := strconv.Atoi(rows[j][2])
			if iN == jN {
				return rows[i][0] > rows[j][0]
			}
			return iN > jN
		},
	)
	var writer *csv.Writer
	file, err := os.Create("summary.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "contributions", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
}

func genReport(roots []string) {
	thrN := runtime.NumCPU()
	// thrN = 1
	// roots = roots[:10]
	runtime.GOMAXPROCS(thrN)
	ch := make(chan []contribReportItem)
	report := contribReport{}
	nThreads := 0
	for _, root := range roots {
		go reportForRoot(ch, root)
		nThreads++
		if nThreads == thrN {
			items := <-ch
			nThreads--
			for _, item := range items {
				report.items = append(report.items, item)
			}
		}
	}
	for nThreads > 0 {
		items := <-ch
		nThreads--
		for _, item := range items {
			report.items = append(report.items, item)
		}
	}
	enrichReport(&report)
	summaryReport(&report)
	saveReport(report.items)
	saveSummaryReport(report.summary)
}

func setupSHDB() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		fatal("DB_URL must be set")
	}
	var err error
	gDB, err = sqlx.Connect("mysql", dbURL)
	fatalError(err)
}

func main() {
	gESURL = os.Getenv("ES_URL")
	if gESURL == "" {
		fatal("ES_URL must be set")
	}
	gOrg = os.Getenv("ORG")
	if gOrg == "" {
		fatal("ORG must be set")
	}
	setupSHDB()
	genReport(getSlugRoots())
}
