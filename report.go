package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	jsoniter "github.com/json-iterator/go"

	_ "github.com/go-sql-driver/mysql"
)

const (
	// cCreatedAtColumn = "metadata__enriched_on"
	// cCreatedAtColumn = "metadata__timestamp"
	cCreatedAtColumn = "metadata__updated_on"
	cOffsetMinutes   = -60
)

var (
	gReport       string
	gESURL        string
	gDB           *sqlx.DB
	gOrg          string
	gFrom         string
	gTo           string
	gIndexFilter  string
	gDbg          bool
	gDatasource   map[string]struct{}
	gSubReport    map[string]struct{}
	gAll          map[string]int
	gDt           time.Time
	gNamePrefix   string
	gMaxThreads   int
	gProgress     bool
	gFiltered     bool
	gIncremental  bool
	gESLogURL     string
	gSyncDates    map[string]time.Time
	gSyncDatesMtx *sync.Mutex
)

var gDtMtx = &sync.Mutex{}

// contributor name, email address, project slug and affiliation date range
type contribReportItem struct {
	uuid       string
	name       string
	email      string
	project    string
	subProject string
	n          int
	from       time.Time
	to         time.Time
	root       string
	commits    int
	locAdded   int
	locDeleted int
	prAct      int
	issueAct   int
}

// contribReport - full report structure
type contribReport struct {
	items   []contribReportItem
	summary map[string]contribReportItem
}

// LOC stats report item (only git)
// subrep
type datalakeLOCReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	sfSlug      string
	createdAt   time.Time
	locAdded    int
	locDeleted  int
	title       string
	url         string
	filtered    bool
}

// Docs stats report item (only confluence)
// subrep
type datalakeDocReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	sfSlug      string
	createdAt   time.Time
	actionType  string // page, new_page, comment, attachment
	title       string
	url         string
	filtered    bool
}

// PRs stats report item (github pull_request & gerrit)
// subrep
type datalakePRReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	sfSlug      string
	createdAt   time.Time
	actionType  string // all possible doc types from github-issue (PRs only) and gerrit, also detecting approvals/rejections/merges
	title       string
	url         string
	filtered    bool
}

// Issues stats report item (github issue, Jira issue, Bugzilla(rest) issue)
// subrep
type datalakeIssueReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	sfSlug      string
	createdAt   time.Time
	actionType  string // all possible doc types from github-issue (Issue only), Jira & Bugzilla(rest)
	title       string
	url         string
	filtered    bool
}

// datalakeReport - full report structure
// subrep
type datalakeReport struct {
	locItems   []datalakeLOCReportItem
	docItems   []datalakeDocReportItem
	prItems    []datalakePRReportItem
	issueItems []datalakeIssueReportItem
}

type resultTypeError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type resultType struct {
	Cursor string          `json:"cursor"`
	Rows   [][]interface{} `json:"rows"`
	Error  resultTypeError `json:"error"`
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

func fatalf(f string, a ...interface{}) {
	fatalError(fmt.Errorf(f, a...))
}

// return values
// 1 - report this error
// 0 - this error is allowed
// -1 - don't report this error, but retry due to missing 'project_slug' column
func reportThisError(err resultTypeError) int {
	errMsg := fmt.Sprintf("%+v", err)
	// We attempt to get all possible data types using project root, if some data type is not present then this is just fine.
	if strings.Contains(errMsg, "Unknown index ") {
		return 0
	}
	// We attempt to get PRs data on github-issue indices, some projects have only issues enabled and not PRs - so this is fine too.
	if strings.Contains(errMsg, "Unknown column ") && strings.Contains(errMsg, "merged_by_data_id") {
		return 0
	}
	// don't report this error, but retry due to missing 'project_slug' column
	if strings.Contains(errMsg, "Unknown column ") && strings.Contains(errMsg, "project_slug") {
		return -1
	}
	return 1
}

func reportThisErrorOrg(err resultTypeError) bool {
	errMsg := fmt.Sprintf("%+v", err)
	// We attempt to get all possible data types using project root, if some data type is not present then this is just fine.
	if strings.Contains(errMsg, "Unknown index ") {
		return false
	}
	return true
}

func getIndices(res map[string]interface{}, aliases bool) (indices []string) {
	ary, _ := res["x"].([]interface{})
	for _, i := range ary {
		item, _ := i.(map[string]interface{})
		idx, _ := item["index"].(string)
		if !strings.HasPrefix(idx, "sds-") {
			continue
		}
		// if strings.HasSuffix(idx, "-raw") || strings.HasSuffix(idx, "-for-merge") || strings.HasSuffix(idx, "-cache") || strings.HasSuffix(idx, "-converted") || strings.HasSuffix(idx, "-temp") || strings.HasSuffix(idx, "-last-action-date-cache") {
		if strings.HasSuffix(idx, "-raw") || strings.HasSuffix(idx, "-cache") || strings.HasSuffix(idx, "-converted") || strings.HasSuffix(idx, "-temp") || strings.HasSuffix(idx, "-last-action-date-cache") {
			continue
		}
		if gIndexFilter != "" && !strings.Contains(idx, gIndexFilter) {
			continue
		}
		// to limit data processing while implementing
		// yyy
		/*
			if !strings.Contains(idx, "o-ran") {
				continue
			}
		*/
		if !aliases {
			sCnt, _ := item["docs.count"].(string)
			cnt, _ := strconv.Atoi(sCnt)
			if gDbg {
				fmt.Printf("%s-> %d\n", idx, cnt)
			}
			if cnt == 0 {
				gAll[idx] = 0
				if gDbg {
					fmt.Printf("skipping an empty index %s\n", idx)
				}
				continue
			}
			indices = append(indices, idx)
			gAll[idx] = cnt
			continue
		}
		cnt, ok := gAll[idx]
		if ok && cnt == 0 {
			if gDbg {
				fmt.Printf("skipping an empty index from alias %s\n", idx)
			}
			continue
		}
		if !ok {
			gAll[idx] = -1
		}
		indices = append(indices, idx)
	}
	sort.Strings(indices)
	if gDbg {
		if aliases {
			fmt.Printf("indices from aliases: %v\n", indices)
		} else {
			fmt.Printf("indices: %v\n", indices)
		}
	}
	return
}

func getRoots(indices, aliases []string) (roots, dsa []string) {
	fmt.Printf("%d indices, %d aliases\n", len(indices), len(aliases))
	dss := make(map[string]struct{})
	all := make(map[string]struct{})
	var reported map[string]struct{}
	if gDbg {
		reported = make(map[string]struct{})
	}
	for _, idx := range indices {
		if strings.HasSuffix(idx, "-for-merge") {
			idx = idx[:len(idx)-10]
		}
		ary := strings.Split(idx, "-")
		lAry := len(ary)
		ds := ary[lAry-1]
		root := strings.Join(ary[1:lAry-1], "-")
		if strings.HasSuffix(root, "-github") {
			root = root[:len(root)-7]
			ds = "github-" + ds
		}
		if root == "" {
			continue
		}
		if gDatasource != nil {
			_, ok := gDatasource[ds]
			if !ok {
				if gDbg {
					_, rep := reported[ds]
					if !rep {
						fmt.Printf("data source '%s' not on the data source list, skipping\n", ds)
						reported[ds] = struct{}{}
					}
				}
				continue
			}
			if gDbg {
				_, rep := reported[ds]
				if !rep {
					fmt.Printf("data source '%s' included\n", ds)
					reported[ds] = struct{}{}
				}
			}
		}
		dss[ds] = struct{}{}
		all[root] = struct{}{}
	}
	for _, idx := range aliases {
		if strings.HasSuffix(idx, "-for-merge") {
			idx = idx[:len(idx)-10]
		}
		ary := strings.Split(idx, "-")
		lAry := len(ary)
		ds := ary[lAry-1]
		root := strings.Join(ary[1:lAry-1], "-")
		if strings.HasSuffix(root, "-github") {
			root = root[:len(root)-7]
			ds = "github-" + ds
		}
		if root == "" {
			continue
		}
		if gDatasource != nil {
			_, ok := gDatasource[ds]
			if !ok {
				if gDbg {
					_, rep := reported[ds]
					if !rep {
						fmt.Printf("data source '%s' not on the data source list, skipping\n", ds)
						reported[ds] = struct{}{}
					}
				}
				continue
			}
			if gDbg {
				_, rep := reported[ds]
				if !rep {
					fmt.Printf("data source '%s' included\n", ds)
					reported[ds] = struct{}{}
				}
			}
		}
		dss[ds] = struct{}{}
		all[root] = struct{}{}
	}
	fmt.Printf("%d data source types\n", len(dss))
	for root := range all {
		roots = append(roots, root)
	}
	for ds := range dss {
		dsa = append(dsa, ds)
	}
	sort.Strings(dsa)
	if gDbg {
		fmt.Printf("data source types: %v\n", dsa)
	}
	sort.Strings(roots)
	fmt.Printf("%d projects detected\n", len(roots))
	if gDbg {
		fmt.Printf("projects: %v\n", roots)
	}
	return
}

func getSlugRoots() (slugRoots, dataSourceTypes []string) {
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
	indices := getIndices(result, false)
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
	aliases := getIndices(result, true)
	slugRoots, dataSourceTypes = getRoots(indices, aliases)
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

func toYMDHMSDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

func applySlugMapping(slug string, useDAWhenNotFound bool) (daName, sfName string, found bool) {
	slugs := []string{slug}
	defer func() {
		if gDbg {
			fmt.Printf("slug mapping: %s -> %+v -> %s,%s,%v\n", slug, slugs, daName, sfName, found)
			return
		}
		if !found {
			fmt.Printf("slug mapping not found: %s -> %+v -> %s,%s\n", slug, slugs, daName, sfName)
		}
	}()
	ary := strings.Split(slug, "-")
	n := len(ary)
	for i := 1; i < n; i++ {
		slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
	}
	if strings.HasSuffix(slug, "-shared") {
		newSlug := slug[:len(slug)-7]
		if gDbg {
			fmt.Printf("shared slug detected: '%s' -> '%s'\n", slug, newSlug)
		}
		slugs = append(slugs, newSlug)
		ary := strings.Split(newSlug, "-")
		n := len(ary)
		for i := 1; i < n; i++ {
			slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
		}
	}
	for _, kw := range []string{"common", "all"} {
		if strings.HasSuffix(slug, "-"+kw) {
			newSlug := slug[:len(slug)-(len(kw)+1)]
			if gDbg {
				fmt.Printf("%s slug detected: '%s' -> '%s'\n", kw, slug, newSlug)
			}
			slugs = append(slugs, newSlug)
			ary := strings.Split(newSlug, "-")
			n := len(ary)
			for i := 1; i < n; i++ {
				slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
			}
			if n > 1 {
				ary = ary[:n-1]
				n--
				newSlug := strings.Join(ary, "-")
				slugs = append(slugs, newSlug)
				if gDbg {
					fmt.Printf("%s slug detected (2nd attempt): '%s' -> '%s'\n", kw, slug, newSlug)
				}
				for i := 1; i < n; i++ {
					slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
				}
			}
		}
	}
	if strings.HasSuffix(slug, "-f") {
		newSlug := slug[:len(slug)-2]
		if gDbg {
			fmt.Printf("dash-f slug detected: '%s' -> '%s'\n", slug, newSlug)
		}
		slugs = append(slugs, newSlug)
		ary := strings.Split(newSlug, "-")
		n := len(ary)
		for i := 1; i < n; i++ {
			slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
		}
	}
	for _, slg := range slugs {
		rows, err := gDB.Query("select sf_name from slug_mapping where da_name = ?", slg)
		fatalError(err)
		for rows.Next() {
			err = rows.Scan(&sfName)
			fatalError(err)
			break
		}
		fatalError(rows.Err())
		fatalError(rows.Close())
		if sfName != "" {
			daName = slg
			found = true
			return
		}
	}
	if useDAWhenNotFound {
		sfName = slug
	}
	daName = slug
	return
}

func getPatternIncrementalDate(key string) string {
	method := "POST"
	data := `{"query":"select max(dt) from \"datalake-status\" where key = '` + key + `'"}`
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	url := gESLogURL + "/_sql?format=json"
	req, err := http.NewRequest(method, url, payloadBody)
	fatalError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	fatalError(err)
	body, err := ioutil.ReadAll(resp.Body)
	fatalError(err)
	_ = resp.Body.Close()
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		if gDbg {
			// fmt.Printf("getPatternIncrementalDate(%s): %s/%s: %v\n", key, url, data, result)
			fmt.Printf("getPatternIncrementalDate: error for %s: %v\n", key, result.Error)
		}
		return ""
	}
	if len(result.Rows) < 1 || len(result.Rows[0]) < 1 {
		return ""
	}
	sDt, ok := result.Rows[0][0].(string)
	if !ok {
		return ""
	}
	cond := " and " + cCreatedAtColumn + " >= '" + sDt + "'"
	if gDbg {
		fmt.Printf("%s condition: %s >= %s\n", key, cCreatedAtColumn, sDt)
	}
	return cond
}

func savePatternIncrementalDate(key string, when time.Time) {
	gSyncDatesMtx.Lock()
	gSyncDates[key] = when
	gSyncDatesMtx.Unlock()
}

func saveIncrementalDates(thrN int) {
	fmt.Printf("saving %d sync dates status using %d threads...\n", len(gSyncDates), thrN)
	type docType struct {
		Dt  time.Time `json:"dt"`
		Key string    `json:"key"`
	}
	save := func(ch chan struct{}, key string, when time.Time) {
		defer func() {
			ch <- struct{}{}
		}()
		data := docType{Key: key, Dt: when}
		payloadBytes, err := jsoniter.Marshal(data)
		fatalError(err)
		payloadBody := bytes.NewReader(payloadBytes)
		method := "POST"
		url := gESLogURL + "/datalake-status/_doc?refresh=wait_for"
		req, err := http.NewRequest(method, os.ExpandEnv(url), payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != 201 {
			body, err := ioutil.ReadAll(resp.Body)
			fatalError(err)
			fatalf("Method:%s url:%s status:%d data:%+v\n%s", method, url, resp.StatusCode, data, body)
		}
		if gDbg {
			fmt.Printf("saved sync date '%s' -> '%v'\n", key, when)
		}
	}
	ch := make(chan struct{})
	nThreads := 0
	for key, when := range gSyncDates {
		go save(ch, key, when)
		nThreads++
		if nThreads == thrN {
			<-ch
			nThreads--
		}
	}
	for nThreads > 0 {
		<-ch
		nThreads--
	}
	fmt.Printf("saved %d sync dates status\n", len(gSyncDates))
}

// subrep
func datalakeLOCReportForRoot(root, projectSlug, sfSlug string, overrideProjectSlug, missingCol bool) (locItems []datalakeLOCReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got LOC %s: %d\n", root, len(locItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":git"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(locItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-git*,-*-github,-*-github-issue,-*-github-repository,-*-github-pull_request,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isGit := fields["git_uuid"]
	if !isGit {
		if gDbg {
			fmt.Printf("%s: has no git data\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"title", "commit_url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select git_uuid, author_id, '%s', %s, lines_added, lines_removed%s from \"%s\" `+
				`where author_id is not null and type = 'commit' and (lines_added > 0 or lines_removed > 0)%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select git_uuid, author_id, project_slug, %s, lines_added, lines_removed%s from \"%s\" `+
				`where author_id is not null and type = 'commit' and (lines_added > 0 or lines_removed > 0)%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeLOCReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [f16103509162d3044c5882a2b0d4c4cf70c16cb0 7f6daae75c73b05795d1cffb2f6642d51ad94ab5 accord/accord-concerto 2021-08-11T12:16:55.000Z 1 1]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			// Skip rows having no enriched date
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			fLOCAdded, _ := row[4].(float64)
			locAdded := int(fLOCAdded)
			fLOCDeleted, _ := row[5].(float64)
			locDeleted := int(fLOCDeleted)
			title, _ := row[6].(string)
			url, _ := row[7].(string)
			item := datalakeLOCReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "git",
				projectSlug: pSlug,
				sfSlug:      sfSlug,
				createdAt:   createdAt,
				locAdded:    locAdded,
				locDeleted:  locDeleted,
				title:       title,
				url:         url,
				filtered:    false,
			}
			locItems = append(locItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeLOCReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeGithubPRReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, missingCol bool) (prItems []datalakePRReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got github-pr %s: %d\n", root, len(prItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":github-pulls"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(prItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-github-issue*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isPR := fields["is_github_pull_request"]
	if !isPR {
		if gDbg {
			fmt.Printf("%s: has no github PRs data (probably only issues?)\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"title", "url", "html_url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, '%s', %s, type, state, merged_by_data_id%s from \"%s\" `+
				`where author_id is not null and is_github_pull_request = 1%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, project_slug, %s, type, state, merged_by_data_id%s from \"%s\" `+
				`where author_id is not null and is_github_pull_request = 1%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeGitHubPRReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [ffd3f84758dbdd21df9a66b09a96df1ae47db0f3 2ca78b1cc8c9bb81152d3495a5aa8058fba5801c academy-software-foundation/opencolorio 2021-09-16T06:33:13.461Z pull_request closed 5675ad72e0958195400713bc2430e77315cbedf9]
			// [ffd3f84758dbdd21df9a66b09a96df1ae47db0f3 5675ad72e0958195400713bc2430e77315cbedf9 academy-software-foundation/opencolorio 2021-09-16T06:33:13.461Z pull_request_review APPROVED <nil>]
			// [293c9ccfe121c0b895c9b7cf0ce20aa11f142aca 2ab3287aa9894fbb6b395e544fc579ba3126c8c7 academy-software-foundation/opencolorio 2021-10-04T23:32:45.981Z issue_comment <nil> <nil>]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			// Skip rows having no enriched date
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			state, _ := row[5].(string)
			mergeIdentityID, _ := row[6].(string)
			// fmt.Printf("(%s,%s,%s)\n", actionType, state, mergeIdentityID)
			if actionType == "pull_request_review" {
				switch state {
				case "APPROVED":
					actionType = "GitHub PR approved"
				case "CHANGES_REQUESTED":
					actionType = "GitHub PR changes requested"
				case "COMMENTED":
					actionType = "GitHub PR review comment"
				case "DISMISSED":
					actionType = "GitHub PR dismissed"
				}
			}
			title, _ := row[7].(string)
			var url string
			if actionType == "pull_request" {
				url, _ = row[8].(string)
			} else {
				url, _ = row[9].(string)
			}
			if identityID == mergeIdentityID {
				actionType = "GitHub PR merged"
			}
			item := datalakePRReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "github",
				projectSlug: pSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  actionType,
				title:       title,
				url:         url,
				filtered:    false,
			}
			prItems = append(prItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeGitHubPRReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeGerritReviewReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, missingCol bool) (prItems []datalakePRReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got gerrit-review %s: %d\n", root, len(prItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":gerrit"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(prItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-gerrit*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isGerrit := fields["type"]
	if !isGerrit {
		if gDbg {
			fmt.Printf("%s: has no gerrit data\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"summary", "url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, '%s', %s, type, approval_value%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, project_slug, %s, type, approval_value%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeGerritReviewReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [78843539ad1642c4ab74ae6c19772d1a40bf951d a006613a2aefee76acceb8f5fb6867f49c32ebe1 o-ran/shared 2021-09-07T05:56:36.913Z approval 1]
			// [03c753252a0c27bd890abf272c93e61bb1e28c31 6183ef1a4ac9e5403ff6bf13a1508654def1bb09 o-ran/shared 2021-09-07T05:56:31.998Z patchset <nil>]
			// [235bff88695c1e049b904edc9b7744357acf11ac 81e04dfee8603594a86b780010f7669650d7f076 lfn/tungsten-fabric 2021-09-07T05:37:36.574Z comment <nil>]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			// Skip rows having no enriched date
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			approvalValue, _ := row[5].(string)
			// fmt.Printf("(%s,%s)\n", actionType, approvalValue)
			if actionType == "approval" {
				switch approvalValue {
				case "-1", "-2", "-3":
					actionType = "Gerrit review rejected"
				case "0", "1", "2", "3":
					actionType = "Gerrit review approved"
				}
			}
			summary, _ := row[6].(string)
			url, _ := row[7].(string)
			item := datalakePRReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "gerrit",
				projectSlug: pSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  actionType,
				title:       summary,
				url:         url,
				filtered:    false,
			}
			prItems = append(prItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeGerritReviewReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// Top contributor functions
func getAllFields(indexPattern string) (fields map[string]struct{}) {
	fields = map[string]struct{}{}
	data := fmt.Sprintf(`{"query":"show columns in \"%s\""}`, jsonEscape(indexPattern))
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := "POST"
	url := gESURL + "/_sql?format=csv"
	req, err := http.NewRequest(method, url, payloadBody)
	fatalError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	fatalError(err)
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			fatalError(fmt.Errorf("readAll non-ok request error: %+v for %s url: %s, data: %s", err, method, url, data))
			return
		}
		fatalError(fmt.Errorf("method:%s url:%s data: %s status:%d\n%s", method, url, data, resp.StatusCode, body))
		return
	}
	reader := csv.NewReader(resp.Body)
	row := []string{}
	n := 0
	for {
		row, err = reader.Read()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			fatalError(fmt.Errorf("read CSV row #%d, error: %v/%T", n, err, err))
			return
		}
		n++
		// hash_short,VARCHAR,keyword
		//if row[1] == "VARCHAR" && row[2] == "keyword" {
		//}
		fields[row[0]] = struct{}{}
	}
	return
}

// subrep
func datalakeGithubIssueReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, missingCol bool) (issueItems []datalakeIssueReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got github-issue %s: %d\n", root, len(issueItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":github-issue"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(issueItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-github-issue*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isIssue := fields["is_github_issue"]
	if !isIssue {
		if gDbg {
			fmt.Printf("%s: has no github issues data (probably only PRs?)\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"title", "url", "html_url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, '%s', %s, type, pull_request%s from \"%s\" `+
				`where author_id is not null and not (pull_request = true) and is_github_issue = 1%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, project_slug, %s, type, pull_request%s from \"%s\" `+
				`where author_id is not null and not (pull_request = true) and is_github_issue = 1%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeGitHubIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [bdf57807ad938a88ba699da2bdf49bf851884dc9 d21d4c49184b31550fd6526863fb64894f437e64 hyperledger/sawtooth 2021-09-07T14:49:35.631Z issue_assignee]
			// [9ff1f7e20274861e9415a5af8e7f330055a658f8 342fc492a4e5fd634c527bb09b3c079f0737e3cd ojsf/ojsf-nodejs 2021-09-07T06:22:45.628Z issue_comment]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			// Skip rows having no enriched date
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			isPull, _ := row[5].(bool)
			if isPull {
				actionType = "pr_" + actionType
			}
			title, _ := row[6].(string)
			var url string
			if actionType == "issue" {
				url, _ = row[7].(string)
			} else {
				url, _ = row[8].(string)
			}
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "github",
				projectSlug: pSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  actionType,
				title:       title,
				url:         url,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeGitHubIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeJiraIssueReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, missingCol bool) (issueItems []datalakeIssueReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got jira issue %s: %d\n", root, len(issueItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":jira"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(issueItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	// can also get status and/or status_category_key
	pattern := jsonEscape("sds-" + root + "-jira*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isJira := fields["type"]
	if !isJira {
		if gDbg {
			fmt.Printf("%s: has no gerrit data\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"summary", "url", "issue_url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, '%s', %s, type%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select id, author_id, project_slug, %s, type%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeJiraIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [3fc01f224df4430fa86a03aa7a7d581210e58f64 30f4c2f64cf70d52ed96b4e2fd7f46ae3b59c728 lfn/pnda 2021-09-07T05:42:55.185Z issue]
			// [053c26541dd50c096e316a38feded0cb1225c051 e439d8d016a641d90ca408b9014afc675d55ad50 hyperledger/cello 2021-09-07T13:57:20.029Z comment]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			// status, _ := row[5].(string)
			summary, _ := row[5].(string)
			var url string
			if actionType == "issue" {
				url, _ = row[6].(string)
			} else {
				url, _ = row[7].(string)
			}
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "jira",
				projectSlug: pSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  "jira_" + actionType,
				title:       summary,
				url:         url,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeJiraIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeBugzillaIssueReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, rest bool) (issueItems []datalakeIssueReportItem) {
	ds := "bugzilla"
	if rest {
		ds += "rest"
	}
	if gDbg {
		defer func() {
			fmt.Printf("got %s issue %s: %d\n", ds, root, len(issueItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":bugzilla"
		if rest {
			key += "rest"
		}
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if len(issueItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-" + ds + "*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isBugzilla := fields["author_id"]
	if !isBugzilla {
		if gDbg {
			fmt.Printf("%s: has no gerrit data\n", root)
		}
		return
	}
	appendCols := ""
	extraCols := []string{"main_description", "url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	// can also get status and/or status_category_key
	// TODO: is uuid an unique document key in bugzilla?
	var data string
	data = fmt.Sprintf(
		`{"query":"select uuid, author_id, %s, status%s from \"%s\" `+
			`where author_id is not null%s","fetch_size":%d}`,
		cCreatedAtColumn,
		appendCols,
		pattern,
		fromCond,
		10000,
	)
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeBugzillaIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [5be4b29396a72501206f561082f2f87f696bc9ef 29dbe15b5c802d2e32e9d02a76798815660005b8 2021-10-04T19:52:27.208Z RESOLVED]
			// [16e6334b1385f7ff83dac3cd322dfd3ecfdc8f5e 77f6e01aaac4e573205fb8e8097b91b253f76067 2021-10-04T19:52:29.152Z UNCONFIRMED]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			// Skip rows having no enriched date
			if row[2] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[2].(string))
			status, _ := row[3].(string)
			desc, _ := row[4].(string)
			ary := strings.Split(desc, "\n")
			if len(ary) > 1 {
				desc = ary[0]
			}
			url, _ := row[5].(string)
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  ds,
				projectSlug: projectSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  "bugzilla:" + status,
				title:       desc,
				url:         url,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeBugzillaIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeDocReportForRoot(root, projectSlug, sfName string, overrideProjectSlug, missingCol bool) (docItems []datalakeDocReportItem, retry bool) {
	if gDbg {
		defer func() {
			fmt.Printf("got docs %s: %d\n", root, len(docItems))
		}()
	}
	var fromCond string
	if gIncremental {
		key := root + ":confluence"
		fromCond = getPatternIncrementalDate(key)
		defer func() {
			if !retry && len(docItems) > 0 {
				savePatternIncrementalDate(key, time.Now().Add(time.Duration(cOffsetMinutes)*time.Minute))
			}
		}()
	}
	pattern := jsonEscape("sds-" + root + "-confluence*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	fields := getAllFields(pattern)
	_, isConfluence := fields["type"]
	if !isConfluence {
		if gDbg {
			fmt.Printf("%s: has no gerrit data\n", root)
		}
		return
	}
	appendCols := ""
	// We have both content_url and url fields, but content_url seems to point to the right (possibly past) version from exact activity date
	extraCols := []string{"title", "content_url"}
	for _, extraCol := range extraCols {
		_, present := fields[extraCol]
		if present {
			appendCols += `, \"` + extraCol + `\"`
		} else {
			appendCols += `, ''`
		}
	}
	method := "POST"
	var data string
	if missingCol {
		data = fmt.Sprintf(
			`{"query":"select uuid, author_id, '%s', %s, type%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			projectSlug,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	} else {
		data = fmt.Sprintf(
			`{"query":"select uuid, author_id, project_slug, %s, type%s from \"%s\" `+
				`where author_id is not null%s","fetch_size":%d}`,
			cCreatedAtColumn,
			appendCols,
			pattern,
			fromCond,
			10000,
		)
	}
	if gDbg {
		fmt.Printf("%s:%s\n", pattern, data)
	}
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		status := reportThisError(result.Error)
		if gDbg || status > 0 {
			fmt.Printf("datalakeDocReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		if status < 0 {
			retry = true
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	var pSlug string
	if overrideProjectSlug {
		pSlug = projectSlug
	}
	processResults := func() {
		skipped := 0
		for _, row := range result.Rows {
			// [cff117ac4b479e31473727cd775f7e96746e7e7e 9e7702dd25aac483d9104b4ba93fa1b73c751083 academy-software-foundation/academy-software-foundation-common 2021-10-01T05:14:53.931Z page]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			// Skip rows having no enriched date
			if row[3] == nil {
				skipped++
				continue
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			title, _ := row[5].(string)
			url, _ := row[6].(string)
			item := datalakeDocReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "confluence",
				projectSlug: pSlug,
				sfSlug:      sfName,
				createdAt:   createdAt,
				actionType:  actionType,
				title:       title,
				url:         url,
				filtered:    false,
			}
			docItems = append(docItems, item)
		}
		if skipped > 0 {
			fmt.Printf("%s: skipped %d/%d rows due to missing %s\n", pattern, skipped, len(result.Rows), cCreatedAtColumn)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("datalakeDocReportForRoot: error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	return
}

// subrep
func datalakeReportForRoot(ch chan datalakeReport, msg, root string, dataSourceTypes []string, bLOC, bDocs, bPRs, bIssues bool) (itemData datalakeReport) {
	defer func() {
		gDtMtx.Lock()
		defer gDtMtx.Unlock()
		now := time.Now()
		ago := now.Sub(gDt)
		// fmt.Printf("%f %s\n", ago.Seconds(), root)
		if ago.Seconds() > 10.0 {
			fmt.Printf("%s current: %s\n", msg, root)
			gDt = now
		}
		ch <- itemData
	}()
	daName, sfName, found := applySlugMapping(root, false)
	var retry bool
	if gDbg {
		fmt.Printf("running for: %s %v -> %s,%s,%v\n", root, dataSourceTypes, daName, sfName, found)
	}
	if bLOC {
		itemData.locItems, retry = datalakeLOCReportForRoot(root, daName, sfName, found, false)
		if retry {
			itemData.locItems, _ = datalakeLOCReportForRoot(root, daName, sfName, found, true)
		}
	}
	if bDocs {
		itemData.docItems, retry = datalakeDocReportForRoot(root, daName, sfName, found, false)
		if retry {
			itemData.docItems, _ = datalakeDocReportForRoot(root, daName, sfName, found, true)
		}
	}
	if bPRs {
		itemData.prItems, retry = datalakeGithubPRReportForRoot(root, daName, sfName, found, false)
		if retry {
			itemData.prItems, _ = datalakeGithubPRReportForRoot(root, daName, sfName, found, true)
		}
		prItems, retry := datalakeGerritReviewReportForRoot(root, daName, sfName, found, false)
		if retry {
			prItems, _ = datalakeGerritReviewReportForRoot(root, daName, sfName, found, true)
		}
		if len(prItems) > 0 {
			itemData.prItems = append(itemData.prItems, prItems...)
		}
	}
	if bIssues {
		itemData.issueItems, retry = datalakeGithubIssueReportForRoot(root, daName, sfName, found, false)
		if retry {
			itemData.issueItems, _ = datalakeGithubIssueReportForRoot(root, daName, sfName, found, true)
		}
		issueItems, retry := datalakeJiraIssueReportForRoot(root, daName, sfName, found, false)
		if retry {
			issueItems, _ = datalakeJiraIssueReportForRoot(root, daName, sfName, found, true)
		}
		if len(issueItems) > 0 {
			itemData.issueItems = append(itemData.issueItems, issueItems...)
		}
		issueItems = datalakeBugzillaIssueReportForRoot(root, daName, sfName, found, false)
		if len(issueItems) > 0 {
			itemData.issueItems = append(itemData.issueItems, issueItems...)
		}
		issueItems = datalakeBugzillaIssueReportForRoot(root, daName, sfName, found, true)
		if len(issueItems) > 0 {
			itemData.issueItems = append(itemData.issueItems, issueItems...)
		}
	}
	return
}

func orgReportForRoot(ch chan []contribReportItem, root string) (items []contribReportItem) {
	defer func() {
		ch <- items
	}()
	_, sfName, _ := applySlugMapping(root, true)
	// fmt.Printf("running for: %s -> %s\n", root, sfName)
	pattern := jsonEscape("sds-" + root + "-*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select author_uuid, count(*) as cnt, min(metadata__updated_on) as f, max(metadata__updated_on) as t, project from \"%s\" `+
			`where author_org_name = '%s' and metadata__updated_on >= '%s' and metadata__updated_on < '%s' group by author_uuid, project","fetch_size":%d}`,
		pattern,
		gOrg,
		gFrom,
		gTo,
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
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Missing index error in this case is allowed
		if gDbg || reportThisErrorOrg(result.Error) {
			fmt.Printf("error for %s: %v\n", pattern, result.Error)
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	processResults := func() {
		for _, row := range result.Rows {
			// [uuidstr 6 2019-06-25T06:07:45.000Z 2019-07-05T23:17:20.000Z subproj]
			uuid, _ := row[0].(string)
			fN, _ := row[1].(float64)
			n := int(fN)
			from, _ := timeParseES(row[2].(string))
			to, _ := timeParseES(row[3].(string))
			subProject, _ := row[4].(string)
			if subProject == "" {
				subProject = "-"
			}
			item := contribReportItem{
				uuid:       uuid,
				n:          n,
				from:       from,
				to:         to,
				project:    sfName,
				subProject: subProject,
				root:       root,
			}
			items = append(items, item)
		}
	}
	processResults()
	page := 1
	for {
		if result.Cursor == "" {
			break
		}
		data = `{"cursor":"` + result.Cursor + `"}`
		//fmt.Printf("data=%s\n", data)
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
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("error for %s: %v\n", pattern, result.Error)
			break
		}
		if len(result.Rows) == 0 {
			break
		}
		page++
		if gDbg || gProgress {
			fmt.Printf("%s: processing #%d page...\n", pattern, page)
		}
		processResults()
	}
	if result.Cursor == "" {
		return
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
	// fmt.Printf("%s -> %s: %v\n", root, sfName, items)
	/*
		if len(items) > 0 {
			fmt.Printf("%s -> %s: %+v\n", root, sfName, items)
		}
	*/
	return
}

func enrichOtherMetrics(ch chan struct{}, mtx *sync.RWMutex, report *contribReport, idx int) {
	defer func() {
		ch <- struct{}{}
	}()
	// other items:
	// commits, loc_added, loc_deleted.
	// prAct: total PR Activity (PRs submitted + reviewed + approved + merged + review comments + PR comments).
	// issueAct: total Issue Activity (issues submitted + issues closed + issue comments), note: we don't have data about who closed an issue.
	mtx.RLock()
	item := report.items[idx]
	root := item.root
	uuid := item.uuid
	subProject := item.subProject
	mtx.RUnlock()
	var projectCond string
	if subProject == "" {
		projectCond = "(project = '' or project is null)"
	} else {
		projectCond = "project = '" + subProject + "'"
	}
	method := "POST"
	url := gESURL + "/_sql?format=json"
	var result resultType
	githubPattern := jsonEscape("sds-" + root + "-github-issue")
	gitPattern := jsonEscape("sds-" + root + "-git")
	processGit := func() {
		data := fmt.Sprintf(
			// FIXME: once da-ds git is eabled
			// `{"query":"select count(distinct hash) as commits, sum(lines_added) as loc_added, sum(lines_removed) as loc_removed from \"%s\" where type = 'commit' and hash is not null and author_uuid = '%s' and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			`{"query":"select count(distinct hash) as commits, sum(lines_added) as loc_added, sum(lines_removed) as loc_removed from \"%s\" where hash is not null and author_uuid = '%s' and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			gitPattern,
			uuid,
			gFrom,
			gTo,
			projectCond,
			10000,
		)
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		req, err := http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		body, err := ioutil.ReadAll(resp.Body)
		fatalError(err)
		_ = resp.Body.Close()
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("error for %s/%s: %v\n", gitPattern, uuid, result.Error)
			return
		}
		if len(result.Rows) == 0 {
			return
		}
		for _, row := range result.Rows {
			// [0 <nil> <nil>] or [159 9097 2295]
			fCommits, _ := row[0].(float64)
			fLOCAdded, _ := row[1].(float64)
			fLOCDeleted, _ := row[2].(float64)
			commits := int(fCommits)
			locAdded := int(fLOCAdded)
			locDeleted := int(fLOCDeleted)
			mtx.Lock()
			report.items[idx].commits = commits
			report.items[idx].locAdded = locAdded
			report.items[idx].locDeleted = locDeleted
			mtx.Unlock()
		}
	}
	processGitHubPR := func() {
		data := fmt.Sprintf(
			// FIXME: if we want more PR related events
			// `{"query":"select count(distinct id) as pr_activity from \"%s\" where type in ('pull_request', 'pull_request_review', 'pull_request_comment', 'pull_request_requested_reviewer', 'pull_request_assignee', 'pull_request_comment_reaction') and (author_uuid = '%s' or user_data_uuid = '%s' or merged_by_data_uuid = '%s') and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			`{"query":"select count(distinct id) as pr_activity from \"%s\" where type in ('pull_request', 'pull_request_review', 'pull_request_comment') and (author_uuid = '%s' or user_data_uuid = '%s' or merged_by_data_uuid = '%s') and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			githubPattern,
			uuid,
			uuid,
			uuid,
			gFrom,
			gTo,
			projectCond,
			10000,
		)
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		req, err := http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		body, err := ioutil.ReadAll(resp.Body)
		fatalError(err)
		_ = resp.Body.Close()
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("error for %s/%s: %v\n", githubPattern, uuid, result.Error)
			return
		}
		if len(result.Rows) == 0 {
			return
		}
		for _, row := range result.Rows {
			// [0] or [<nil>]
			fAct, _ := row[0].(float64)
			act := int(fAct)
			mtx.Lock()
			report.items[idx].prAct = act
			mtx.Unlock()
		}
	}
	processGitHubIssue := func() {
		data := fmt.Sprintf(
			// FIXME: if we want more issue related events
			//`{"query":"select count(distinct id) as issue_activity from \"%s\" where type in ('issue', 'issue_comment', 'issue_assignee', 'issue_comment_reaction', 'issue_reaction') and (author_uuid = '%s' or user_data_uuid = '%s') and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			`{"query":"select count(distinct id) as issue_activity from \"%s\" where type in ('issue', 'issue_comment') and (author_uuid = '%s' or user_data_uuid = '%s') and metadata__updated_on >= '%s' and metadata__updated_on < '%s' and %s","fetch_size":%d}`,
			githubPattern,
			uuid,
			uuid,
			gFrom,
			gTo,
			projectCond,
			10000,
		)
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		req, err := http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		body, err := ioutil.ReadAll(resp.Body)
		fatalError(err)
		_ = resp.Body.Close()
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("error for %s/%s: %v\n", githubPattern, uuid, result.Error)
			return
		}
		if len(result.Rows) == 0 {
			return
		}
		for _, row := range result.Rows {
			// [0] or [<nil>]
			fAct, _ := row[0].(float64)
			act := int(fAct)
			mtx.Lock()
			report.items[idx].issueAct = act
			mtx.Unlock()
		}
	}
	_, ok := gAll[gitPattern]
	if ok {
		processGit()
	}
	_, ok = gAll[githubPattern]
	if ok {
		processGitHubPR()
		processGitHubIssue()
	}
}

func enrichOrgReport(report *contribReport) {
	mtx := &sync.RWMutex{}
	shCh := make(chan struct{})
	go func(ch chan struct{}) {
		defer func() {
			ch <- struct{}{}
		}()
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
			mtx.Lock()
			report.items[i].name = data[0]
			report.items[i].email = data[1]
			mtx.Unlock()
		}
	}(shCh)
	// Enrich other per-author metrics
	thrN := runtime.NumCPU()
	if gMaxThreads > 0 && thrN > gMaxThreads {
		thrN = gMaxThreads
	}
	ch := make(chan struct{})
	nThreads := 0
	for i := range report.items {
		go enrichOtherMetrics(ch, mtx, report, i)
		nThreads++
		if nThreads == thrN {
			<-ch
			nThreads--
		}
	}
	for nThreads > 0 {
		<-ch
		nThreads--
	}
	<-shCh
}

func summaryOrgReport(report *contribReport) {
	// Summary is for all projects combined
	// name,email,project,sub_project,contributions,commits,loc_added,loc_deleted,pr_activity,issue_activity,from,to,uuid -> name,email,contributions,commits,loc_added,loc_deleted,pr_activity,issue_activity,from,to,uuid
	report.summary = make(map[string]contribReportItem)
	for _, item := range report.items {
		uuid := item.uuid
		sItem, ok := report.summary[uuid]
		if !ok {
			sItem = item
			sItem.project = ""
			sItem.subProject = ""
			report.summary[uuid] = sItem
			continue
		}
		sItem.n += item.n
		sItem.commits += item.commits
		sItem.locAdded += item.locAdded
		sItem.locDeleted += item.locDeleted
		sItem.prAct += item.prAct
		sItem.issueAct += item.issueAct
		if sItem.from.After(item.from) {
			sItem.from = item.from
		}
		if sItem.to.Before(item.to) {
			sItem.to = item.to
		}
		report.summary[uuid] = sItem
	}
}

func saveOrgReport(report []contribReportItem) {
	rows := [][]string{}
	for _, item := range report {
		row := []string{
			item.name,
			item.email,
			item.project,
			item.subProject,
			strconv.Itoa(item.n),
			strconv.Itoa(item.commits),
			strconv.Itoa(item.locAdded),
			strconv.Itoa(item.locDeleted),
			strconv.Itoa(item.prAct),
			strconv.Itoa(item.issueAct),
			toMDYDate(item.from),
			toMDYDate(item.to),
			item.uuid,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			iN, _ := strconv.Atoi(rows[i][4])
			jN, _ := strconv.Atoi(rows[j][4])
			if iN == jN {
				for k := 0; k <= 3; k++ {
					if rows[i][k] != rows[j][k] {
						return rows[i][k] > rows[j][k]
					}
				}
			}
			return iN > jN
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "report.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "project", "sub_project", "contributions", "commits", "loc_added", "loc_deleted", "pr_activity", "issue_activity", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

func saveSummaryOrgReport(report map[string]contribReportItem) {
	rows := [][]string{}
	for _, item := range report {
		row := []string{
			item.name,
			item.email,
			strconv.Itoa(item.n),
			strconv.Itoa(item.commits),
			strconv.Itoa(item.locAdded),
			strconv.Itoa(item.locDeleted),
			strconv.Itoa(item.prAct),
			strconv.Itoa(item.issueAct),
			toMDYDate(item.from),
			toMDYDate(item.to),
			item.uuid,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			iN, _ := strconv.Atoi(rows[i][2])
			jN, _ := strconv.Atoi(rows[j][2])
			if iN == jN {
				for k := 0; k <= 1; k++ {
					if rows[i][k] != rows[j][k] {
						return rows[i][k] > rows[j][k]
					}
				}
			}
			return iN > jN
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "summary.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "contributions", "commits", "loc_added", "loc_deleted", "pr_activity", "issue_activity", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

// subrep
func saveDatalakeLOCReport(report []datalakeLOCReportItem) {
	rows := [][]string{}
	for _, item := range report {
		filtered := "0"
		if item.filtered {
			filtered = "1"
		}
		row := []string{
			item.docID,
			item.identityID,
			item.dataSource,
			item.projectSlug,
			item.sfSlug,
			toYMDHMSDate(item.createdAt),
			strconv.Itoa(item.locAdded),
			strconv.Itoa(item.locDeleted),
			item.title,
			item.url,
		}
		if gFiltered {
			row = append(row, filtered)
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][5] > rows[j][5]
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "datalake_loc.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	if gFiltered {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "loc_added", "loc_deleted", "title", "url", "filtered"}))
	} else {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "loc_added", "loc_deleted", "title", "url"}))
	}
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

func toIssueType(inType string) string {
	switch inType {
	case "issue":
		return "GitHub issue created"
	case "issue_assignee":
		return "GitHub issue assignment"
	case "issue_comment":
		return "GitHub issue comment"
	case "issue_comment_reaction":
		return "GitHub issue comment reaction"
	case "issue_reaction":
		return "GitHub issue reaction"
	case "pr_issue":
		return "GitHub PR created"
	case "pr_issue_assignee":
		return "GitHub PR assignment"
	case "pr_issue_comment":
		return "GitHub PR comment"
	case "pr_issue_comment_reaction":
		return "GitHub PR comment reaction"
	case "pr_issue_reaction":
		return "GitHub PR reaction"
	case "jira_issue":
		return "Jira issue created"
	case "jira_comment":
		return "Jira comment added"
	default:
		if strings.HasPrefix(inType, "bugzilla:") {
			status := inType[9:]
			return "Bugzilla issue " + strings.Replace(strings.ToLower(strings.TrimSpace(status)), "_", " ", -1)
		}
		return "?"
	}
}

func toPRType(inType string) string {
	switch inType {
	case "pull_request":
		return "GitHub PR submitted"
	case "pull_request_assignee":
		return "GitHub PR assignment"
	case "pull_request_comment":
		return "GitHub PR comment"
	case "pull_request_comment_reaction":
		return "GitHub PR reaction"
	case "pull_request_requested_reviewer":
		return "GitHub PR requested reviewer assignment"
	case "pull_request_review":
		// should not happen - we're splitting this into separate approval states
		return "GitHub PR review"
	case "GitHub PR approved", "GitHub PR changes requested", "GitHub PR dismissed", "GitHub PR review comment", "GitHub PR merged", "Gerrit review rejected", "Gerrit review approved":
		return inType
	case "approval":
		// should not happen - we're splitting this into separate approval states
		return "Gerrit approval added"
	case "changeset":
		return "Gerrit change set created"
	case "comment":
		return "Gerrit comment"
	case "patchset":
		return "Gerrit patch set created"
	default:
		fmt.Printf("unknown PR action type: %s\n", inType)
		return "?"
	}
}

// subrep
func saveDatalakePRsReport(report []datalakePRReportItem) {
	rows := [][]string{}
	for _, item := range report {
		filtered := "0"
		if item.filtered {
			filtered = "1"
		}
		row := []string{
			item.docID,
			item.identityID,
			item.dataSource,
			item.projectSlug,
			item.sfSlug,
			toYMDHMSDate(item.createdAt),
			toPRType(item.actionType),
			item.title,
			item.url,
		}
		if gFiltered {
			row = append(row, filtered)
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][5] > rows[j][5]
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "datalake_prs.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	if gFiltered {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url", "filtered"}))
	} else {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url"}))
	}
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

// subrep
func saveDatalakeIssuesReport(report []datalakeIssueReportItem) {
	rows := [][]string{}
	for _, item := range report {
		filtered := "0"
		if item.filtered {
			filtered = "1"
		}
		row := []string{
			item.docID,
			item.identityID,
			item.dataSource,
			item.projectSlug,
			item.sfSlug,
			toYMDHMSDate(item.createdAt),
			toIssueType(item.actionType),
			item.title,
			item.url,
		}
		if gFiltered {
			row = append(row, filtered)
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][5] > rows[j][5]
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "datalake_issues.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	if gFiltered {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url", "filtered"}))
	} else {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url"}))
	}
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

func toDocType(inType string) string {
	switch inType {
	case "attachment":
		return "Confluence attachment added"
	case "comment":
		return "Confluence page commented"
	case "new_page":
		return "Confluence page created"
	case "page":
		return "Confluence page edited"
	case "blogpost":
		return "Confluence blog post created"
	default:
		// If any other type of document activity is identified then mark the activity as 'Other'
		return "Other"
	}
}

// subrep
func saveDatalakeDocsReport(report []datalakeDocReportItem) {
	rows := [][]string{}
	for _, item := range report {
		filtered := "0"
		if item.filtered {
			filtered = "1"
		}
		row := []string{
			item.docID,
			item.identityID,
			item.dataSource,
			item.projectSlug,
			item.sfSlug,
			toYMDHMSDate(item.createdAt),
			toDocType(item.actionType),
			item.title,
			item.url,
		}
		if gFiltered {
			row = append(row, filtered)
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][5] > rows[j][5]
		},
	)
	var writer *csv.Writer
	fn := gNamePrefix + "datalake_docs.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	if gFiltered {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url", "filtered"}))
	} else {
		fatalError(writer.Write([]string{"document_id", "identity_id", "datasource", "insights_project_slug", "project_slug", "created_at", "type", "title", "url"}))
	}
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
	fmt.Printf("%s saved\n", fn)
}

func genOrgReport(roots, dataSourceTypes []string) {
	thrN := runtime.NumCPU()
	if gMaxThreads > 0 && thrN > gMaxThreads {
		thrN = gMaxThreads
	}
	// thrN = 1
	// if len(roots) > 20 {
	//	 roots = roots[:20]
	// }
	runtime.GOMAXPROCS(thrN)
	ch := make(chan []contribReportItem)
	report := contribReport{}
	nThreads := 0
	for _, root := range roots {
		go orgReportForRoot(ch, root)
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
	enrichOrgReport(&report)
	summaryOrgReport(&report)
	saveOrgReport(report.items)
	saveSummaryOrgReport(report.summary)
}

// subrep
func dedupDatalakeReport(report *datalakeReport, bLOC, bDocs, bPRs, bIssues bool) {
	if bLOC {
		locDocIDs := map[string]struct{}{}
		locItems := []datalakeLOCReportItem{}
		for _, locItem := range report.locItems {
			_, ok := locDocIDs[locItem.docID]
			if !ok {
				locItems = append(locItems, locItem)
				locDocIDs[locItem.docID] = struct{}{}
			}
		}
		if len(locItems) != len(report.locItems) {
			fmt.Printf("LOC data dedup: %d -> %d (%.2f%%)\n", len(report.locItems), len(locItems), (float64(len(locItems))*100.0)/float64(len(report.locItems)))
			report.locItems = locItems
		} else {
			fmt.Printf("no duplicate elements found, LOC items: %d\n", len(report.locItems))
		}
	}
	if bDocs {
		docDocIDs := map[string]struct{}{}
		docItems := []datalakeDocReportItem{}
		for _, docItem := range report.docItems {
			_, ok := docDocIDs[docItem.docID]
			if !ok {
				docItems = append(docItems, docItem)
				docDocIDs[docItem.docID] = struct{}{}
			}
		}
		if len(docItems) != len(report.docItems) {
			fmt.Printf("Docs data dedup: %d -> %d (%.2f%%)\n", len(report.docItems), len(docItems), (float64(len(docItems))*100.0)/float64(len(report.docItems)))
			report.docItems = docItems
		} else {
			fmt.Printf("no duplicate elements found, docs items: %d\n", len(report.docItems))
		}
	}
	if bPRs {
		prDocIDs := map[string]struct{}{}
		prItems := []datalakePRReportItem{}
		for _, prItem := range report.prItems {
			_, ok := prDocIDs[prItem.docID]
			if !ok {
				prItems = append(prItems, prItem)
				prDocIDs[prItem.docID] = struct{}{}
			}
		}
		if len(prItems) != len(report.prItems) {
			fmt.Printf("PR data dedup: %d -> %d (%.2f%%)\n", len(report.prItems), len(prItems), (float64(len(prItems))*100.0)/float64(len(report.prItems)))
			report.prItems = prItems
		} else {
			fmt.Printf("no duplicate elements found, PR items: %d\n", len(report.prItems))
		}
	}
	if bIssues {
		issueDocIDs := map[string]struct{}{}
		issueItems := []datalakeIssueReportItem{}
		for _, issueItem := range report.issueItems {
			_, ok := issueDocIDs[issueItem.docID]
			if !ok {
				issueItems = append(issueItems, issueItem)
				issueDocIDs[issueItem.docID] = struct{}{}
			}
		}
		if len(issueItems) != len(report.issueItems) {
			fmt.Printf("Issue data dedup: %d -> %d (%.2f%%)\n", len(report.issueItems), len(issueItems), (float64(len(issueItems))*100.0)/float64(len(report.issueItems)))
			report.issueItems = issueItems
		} else {
			fmt.Printf("no duplicate elements found, issue items: %d\n", len(report.issueItems))
		}
	}
}

// subrep
func filterDatalakeReport(report *datalakeReport, identityIDs map[string]struct{}, bLOC, bDocs, bPRs, bIssues bool) {
	if !gFiltered {
		return
	}
	if bLOC {
		locFiltered := 0
		for i, locItem := range report.locItems {
			_, ok := identityIDs[locItem.identityID]
			if !ok {
				report.locItems[i].filtered = true
				locFiltered++
			}
		}
		if locFiltered > 0 {
			fmt.Printf("filtered %d/%d loc items (%.2f%%)\n", locFiltered, len(report.locItems), (float64(locFiltered)*100.0)/float64(len(report.locItems)))
		} else {
			fmt.Printf("all %d LOC items also present in SH DB loc items\n", len(report.locItems))
		}
	}
	if bDocs {
		docFiltered := 0
		for i, docItem := range report.docItems {
			_, ok := identityIDs[docItem.identityID]
			if !ok {
				report.docItems[i].filtered = true
				docFiltered++
			}
		}
		if docFiltered > 0 {
			fmt.Printf("filtered %d/%d docs items (%.2f%%)\n", docFiltered, len(report.docItems), (float64(docFiltered)*100.0)/float64(len(report.docItems)))
		} else {
			fmt.Printf("all %d docs items also present in SH DB doc items\n", len(report.docItems))
		}
	}
	if bPRs {
		prFiltered := 0
		for i, prItem := range report.prItems {
			_, ok := identityIDs[prItem.identityID]
			if !ok {
				report.prItems[i].filtered = true
				prFiltered++
			}
		}
		if prFiltered > 0 {
			fmt.Printf("filtered %d/%d PR items (%.2f%%)\n", prFiltered, len(report.prItems), (float64(prFiltered)*100.0)/float64(len(report.prItems)))
		} else {
			fmt.Printf("all %d PRs items also present in SH DB PR items\n", len(report.prItems))
		}
	}
	if bIssues {
		issueFiltered := 0
		for i, issueItem := range report.issueItems {
			_, ok := identityIDs[issueItem.identityID]
			if !ok {
				report.issueItems[i].filtered = true
				issueFiltered++
			}
		}
		if issueFiltered > 0 {
			fmt.Printf("filtered %d/%d issue items (%.2f%%)\n", issueFiltered, len(report.issueItems), (float64(issueFiltered)*100.0)/float64(len(report.issueItems)))
		} else {
			fmt.Printf("all %d issues items also present in SH DB issue items\n", len(report.issueItems))
		}
	}
}

func getAffiliatedNonBotIdentities(ch chan map[string]struct{}) {
	identityIDs := map[string]struct{}{}
	defer func() {
		ch <- identityIDs
	}()
	// Identity must be present in SH DB
	// It must have at least one enrollment (not matter individual, company) also enrollment dates doesn't matter
	// Identity's profile must not be bot
	query := "select distinct i.id from identities i " +
		"where i.uuid in (select uuid from enrollments) " +
		"and i.uuid not in (select uuid from profiles where is_bot is true)"
	rows, err := gDB.Query(query)
	fatalError(err)
	var id string
	for rows.Next() {
		err = rows.Scan(&id)
		fatalError(err)
		identityIDs[id] = struct{}{}
	}
	fatalError(rows.Err())
	fatalError(rows.Close())
}

func genDatalakeReport(roots, dataSourceTypes []string) {
	if gDbg {
		fmt.Printf("Datalake report for data source types %+v\n", dataSourceTypes)
		fmt.Printf("Datalake report for projects %+v\n", roots)
	}
	thrN := runtime.NumCPU()
	if gMaxThreads > 0 && thrN > gMaxThreads {
		thrN = gMaxThreads
	}
	// thrN = 1
	// if len(roots) > 20 {
	//	 roots = roots[:20]
	// }
	runtime.GOMAXPROCS(thrN)
	var chIdentIDs chan map[string]struct{}
	if gFiltered {
		chIdentIDs = make(chan map[string]struct{})
		go getAffiliatedNonBotIdentities(chIdentIDs)
	}
	ch := make(chan datalakeReport)
	report := datalakeReport{}
	nThreads := 0
	// subrep
	_, bLOC := gSubReport["loc"]
	_, bDocs := gSubReport["docs"]
	_, bPRs := gSubReport["prs"]
	_, bIssues := gSubReport["issues"]
	processData := func() {
		data := <-ch
		nThreads--
		// subrep
		if bLOC {
			for _, item := range data.locItems {
				report.locItems = append(report.locItems, item)
			}
		}
		if bDocs {
			for _, item := range data.docItems {
				report.docItems = append(report.docItems, item)
			}
		}
		if bPRs {
			for _, item := range data.prItems {
				report.prItems = append(report.prItems, item)
			}
		}
		if bIssues {
			for _, item := range data.issueItems {
				report.issueItems = append(report.issueItems, item)
			}
		}
	}
	n := len(roots)
	for i, root := range roots {
		// subrep
		msg := fmt.Sprintf("%d/%d (%.1f%%)", i, n, (float64(i)*100.0)/float64(n))
		go datalakeReportForRoot(ch, msg, root, dataSourceTypes, bLOC, bDocs, bPRs, bIssues)
		nThreads++
		if nThreads == thrN {
			processData()
		}
	}
	for nThreads > 0 {
		processData()
	}
	// subrep
	dedupDatalakeReport(&report, bLOC, bDocs, bPRs, bIssues)
	if gFiltered {
		identityIDs := <-chIdentIDs
		fmt.Printf("%d non-bot identities having at least one enrollment present in SH DB\n", len(identityIDs))
		filterDatalakeReport(&report, identityIDs, bLOC, bDocs, bPRs, bIssues)
	}
	if bLOC {
		saveDatalakeLOCReport(report.locItems)
	}
	if bDocs {
		saveDatalakeDocsReport(report.docItems)
	}
	if bPRs {
		saveDatalakePRsReport(report.prItems)
	}
	if bIssues {
		saveDatalakeIssuesReport(report.issueItems)
	}
	if gIncremental {
		saveIncrementalDates(thrN)
	}
}

func getIncorrectEmailDocs(ch chan [][3]string, index string, ids map[string]string) {
	docs := [][3]string{}
	total := 0
	defer func() {
		if total > 0 {
			fmt.Printf("%s: found %d documents\n", index, total)
		}
		ch <- docs
	}()
	fields := getAllFields(index)
	_, hasAid := fields["author_id"]
	if !hasAid {
		if gDbg {
			fmt.Printf("%s doesn't have author_id field, skipping\n", index)
		}
		return
	}
	var scroll *string
	// Defer free scroll
	defer func() {
		if scroll == nil {
			return
		}
		method := "DELETE"
		data := `{"scroll_id":"` + *scroll + `"}`
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		url := gESURL + "/_search/scroll"
		req, err := http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		body, err := ioutil.ReadAll(resp.Body)
		fatalError(err)
		_ = resp.Body.Close()
		var result interface{}
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if resp.StatusCode != 200 {
			fmt.Printf("%s free scroll error: status %d: %+v\n", index, resp.StatusCode, result)
		}
	}()
	aIDs := "["
	for aid := range ids {
		aIDs += `"` + aid + `",`
	}
	aIDs = aIDs[:len(aIDs)-1] + "]"
	method := "POST"
	packs := 0
	// Scroll search
	for {
		var (
			url          string
			payloadBytes []byte
		)
		if scroll == nil {
			url = gESURL + "/" + index + "/_search?scroll=15m&size=1000"
			payloadBytes = []byte(`{"query":{"bool":{"filter":{"terms":{"author_id":` + aIDs + `}}}}}`)
		} else {
			url = gESURL + "/_search/scroll"
			payloadBytes = []byte(`{"scroll":"15m","scroll_id":"` + *scroll + `"}`)
			packs++
			if gDbg {
				fmt.Printf("%s: using scroll #%d time\n", index, packs)
			}
		}
		payloadBody := bytes.NewReader(payloadBytes)
		req, err := http.NewRequest(method, url, payloadBody)
		fatalError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		fatalError(err)
		if resp.StatusCode != 200 {
			fatalError(fmt.Errorf("%s: status code: %d", index, resp.StatusCode))
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		fatalError(err)
		_ = resp.Body.Close()
		var res interface{}
		err = jsoniter.Unmarshal(body, &res)
		fatalError(err)
		sScroll, ok := res.(map[string]interface{})["_scroll_id"].(string)
		if !ok {
			fatalError(fmt.Errorf("%s: missing _scroll_id in the response %+v", index, res))
			return
		}
		scroll = &sScroll
		items, ok := res.(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})
		if !ok {
			fatalError(fmt.Errorf("%s: missing hits.hits in the response %+v", index, res))
			return
		}
		nItems := len(items)
		if nItems == 0 {
			break
		}
		for _, iitem := range items {
			item, ok := iitem.(map[string]interface{})
			if !ok {
				fmt.Printf("%s: incorrect row (non interface): %+v\n", index, iitem)
				continue
			}
			source, ok := item["_source"].(map[string]interface{})
			if !ok {
				fmt.Printf("%s: incorrect row (no _source): %+v\n", index, item)
				continue
			}
			id, _ := item["_id"].(string)
			idx, _ := item["_index"].(string)
			aid, _ := source["author_id"].(string)
			if id == "" || idx == "" || aid == "" {
				fmt.Printf("%s: incorrect row (no _index, _id or author_id): %+v\n", index, item)
				continue
			}
			doc := [3]string{idx, aid, id}
			if gDbg {
				fmt.Printf("%s: %+v\n", index, doc)
			}
			docs = append(docs, doc)
			total++
		}
	}
}

func genIncorrectEmailsReport() {
	// SH part
	query := `select source, id, email from identities where email is not null and trim(email) != '' and email not regexp '^[][a-zA-Z0-9.!#$%&*+\'\` +
		"`" + `\/=?^_{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'` +
		`order by source, email`
	rows, err := gDB.Query(query)
	fatalError(err)
	var (
		id     string
		source string
		email  string
	)
	ids := map[string]string{}
	data := map[[2]string]struct{}{}
	for rows.Next() {
		err = rows.Scan(&source, &id, &email)
		fatalError(err)
		ids[id] = email
		data[[2]string{source, email}] = struct{}{}
	}
	fatalError(rows.Err())
	fatalError(rows.Close())
	rrows := [][2]string{}
	for row := range data {
		rrows = append(rrows, row)
	}
	sort.Slice(
		rrows,
		func(i, j int) bool {
			if rrows[i][0] == rrows[j][0] {
				return rrows[i][1] < rrows[j][1]
			}
			return rrows[i][0] < rrows[j][0]
		},
	)
	var writer *csv.Writer
	fn := "incorrect_emails.csv"
	file, err := os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"source", "incorrect email"}))
	for _, row := range rrows {
		fatalError(writer.Write([]string{row[0], row[1]}))
	}
	fmt.Printf("%s saved\n", fn)
	// ES part
	// Get all SDS indices
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
	indices := getIndices(result, false)
	// Process them in parallel
	thrN := runtime.NumCPU()
	if gMaxThreads > 0 && thrN > gMaxThreads {
		thrN = gMaxThreads
	}
	runtime.GOMAXPROCS(thrN)
	fmt.Printf("Checking %d indices for %d incorrect emails using %d threads\n", len(indices), len(ids), thrN)
	ch := make(chan [][3]string)
	docs := [][3]string{}
	nThreads := 0
	for _, idx := range indices {
		go getIncorrectEmailDocs(ch, idx, ids)
		nThreads++
		if nThreads == thrN {
			items := <-ch
			nThreads--
			for _, item := range items {
				docs = append(docs, item)
			}
		}
	}
	for nThreads > 0 {
		items := <-ch
		nThreads--
		for _, item := range items {
			docs = append(docs, item)
		}
	}
	sort.Slice(
		docs,
		func(i, j int) bool {
			if docs[i][0] == docs[j][0] {
				if docs[i][1] == docs[j][1] {
					return docs[i][2] < docs[j][2]
				}
				return docs[i][1] < docs[j][1]
			}
			return docs[i][0] < docs[j][0]
		},
	)
	fn = "incorrect_docs.csv"
	file, err = os.Create(fn)
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"index", "author ID", "document ID", "email"}))
	for _, row := range docs {
		email, _ = ids[row[1]]
		fatalError(writer.Write([]string{row[0], row[1], row[2], email}))
	}
	fmt.Printf("%s saved\n", fn)
	// All at once - impossible due to incompatible mappings, can be possible on smaller patterns
	/*
		pattern := jsonEscape("sds-*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
		fields := getAllFields(pattern)
		method := "POST"
		query = `{"query":"select `
		for field := range fields {
			query += `\"` + field + `\", `
		}
		query = query[:len(query)-2] + ` from \"` + pattern + `\" where author_id in (`
		for id := range ids {
			query += `'` + id + `', `
		}
		query = query[:len(query)-2] + `)","fetch_size":10000}`
		if gDbg {
			fmt.Printf("%s:%s\n", pattern, query)
		}
		payloadBytes := []byte(query)
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
		var result resultType
		err = jsoniter.Unmarshal(body, &result)
		fatalError(err)
		if result.Error.Type != "" || result.Error.Reason != "" {
			fmt.Printf("genIncorrectEmailsReport: error for %s: %v\n", pattern, result.Error)
			return
		}
	*/
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

func setupEnvs() {
	gIndexFilter = os.Getenv("ES_INDEX_FILTER")
	gDbg = os.Getenv("DBG") != ""
	gProgress = os.Getenv("PROGRESS") != ""
	gReport = os.Getenv("REPORT")
	if gReport == "" {
		fatal("REPORT must be set")
	}
	gESURL = os.Getenv("ES_URL")
	if gESURL == "" {
		fatal("ES_URL must be set")
	}
	sDSes := strings.TrimSpace(os.Getenv("DATASOURCES"))
	if sDSes != "" {
		ary := strings.Split(sDSes, ",")
		gDatasource = make(map[string]struct{})
		for _, ds := range ary {
			gDatasource[strings.TrimSpace(ds)] = struct{}{}
		}
	}
	gNamePrefix = os.Getenv("NAME_PREFIX")
	if gNamePrefix != "" && !strings.HasSuffix(gNamePrefix, "_") {
		gNamePrefix += "_"
	}
	sMaxThreads := os.Getenv("MAX_THREADS")
	if sMaxThreads != "" {
		var err error
		gMaxThreads, err = strconv.Atoi(sMaxThreads)
		fatalError(err)
		if gMaxThreads < 0 {
			gMaxThreads = 0
		}
		fmt.Printf("Limit threads to: %d\n", gMaxThreads)
	}
	switch gReport {
	case "org":
		gOrg = os.Getenv("ORG")
		if gOrg == "" {
			fatal("ORG must be set")
		}
		gOrg = jsonEscape(gOrg)
		gFrom = jsonEscape(os.Getenv("FROM"))
		gTo = jsonEscape(os.Getenv("TO"))
		if gFrom == "" {
			gFrom = "1900-01-01T00:00:00"
		}
		if gTo == "" {
			gTo = "2100-01-01T00:00:00"
		}
	case "datalake":
		subs := strings.TrimSpace(os.Getenv("SUB_REPORTS"))
		if subs == "" {
			subs = "loc,prs,issues,docs"
		}
		ary := strings.Split(subs, ",")
		// subrep
		gSubReport = make(map[string]struct{})
		for _, sub := range ary {
			gSubReport[strings.TrimSpace(sub)] = struct{}{}
		}
		gFiltered = os.Getenv("FILTERED") != ""
		gIncremental = os.Getenv("INCREMENTAL") != ""
		if gIncremental {
			gESLogURL = os.Getenv("ES_LOG_URL")
			if gESLogURL == "" {
				fatal("ES_LOG_URL must be set when using incremental mode")
			}
			gSyncDates = map[string]time.Time{}
			gSyncDatesMtx = &sync.Mutex{}
		}
	case "incorrect-emails":
	default:
		fatal("unknown report type: " + gReport)
	}
	gAll = make(map[string]int)
	gDt = time.Now()
}

func main() {
	setupEnvs()
	setupSHDB()
	switch gReport {
	case "org":
		genOrgReport(getSlugRoots())
	case "datalake":
		genDatalakeReport(getSlugRoots())
	case "incorrect-emails":
		genIncorrectEmailsReport()
	}
}
