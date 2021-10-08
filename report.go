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
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	jsoniter "github.com/json-iterator/go"

	_ "github.com/go-sql-driver/mysql"
)

var (
	gReport     string
	gESURL      string
	gDB         *sqlx.DB
	gOrg        string
	gFrom       string
	gTo         string
	gDbg        bool
	gDatasource map[string]struct{}
	gAll        map[string]struct{}
)

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
		if strings.HasSuffix(idx, "-raw") || strings.HasSuffix(idx, "-for-merge") || strings.HasSuffix(idx, "-cache") || strings.HasSuffix(idx, "-converted") || strings.HasSuffix(idx, "-temp") || strings.HasSuffix(idx, "-last-action-date-cache") {
			continue
		}
		indices = append(indices, idx)
		gAll[idx] = struct{}{}
	}
	sort.Strings(indices)
	if gDbg {
		fmt.Printf("indices: %v\n", indices)
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

func applySlugMapping(slug string) (sfName string) {
	slugs := []string{slug}
	/*
		defer func() {
			if sfName != slug {
				fmt.Printf("slug mapping: %s -> %+v -> %s\n", slug, slugs, sfName)
			}
		}()
	*/
	ary := strings.Split(slug, "-")
	n := len(ary)
	for i := 1; i < n; i++ {
		slugs = append(slugs, strings.Join(ary[:i], "-")+"/"+strings.Join(ary[i:], "-"))
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
			return
		}
	}
	sfName = slug
	return
}

func reportForRoot(ch chan []contribReportItem, root string) (items []contribReportItem) {
	defer func() {
		ch <- items
	}()
	sfName := applySlugMapping(root)
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
	type resultType struct {
		Cursor string          `json:"cursor"`
		Rows   [][]interface{} `json:"rows"`
		Error  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	fatalError(err)
	if result.Error.Type != "" || result.Error.Reason != "" {
		// Error in this case is allowed
		if gDbg {
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
	type resultType struct {
		Cursor string          `json:"cursor"`
		Rows   [][]interface{} `json:"rows"`
		Error  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
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

func enrichReport(report *contribReport) {
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
	if thrN > 12 {
		thrN = 12
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

func summaryReport(report *contribReport) {
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

func saveReport(report []contribReportItem) {
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
	file, err := os.Create("report.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "project", "sub_project", "contributions", "commits", "loc_added", "loc_deleted", "pr_activity", "issue_activity", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
}

func saveSummaryReport(report map[string]contribReportItem) {
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
	file, err := os.Create("summary.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"name", "email", "contributions", "commits", "loc_added", "loc_deleted", "pr_activity", "issue_activity", "from", "to", "uuid"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
}

func genOrgReport(roots, dataSourceTypes []string) {
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

func genDatalakeReport(roots, dataSourceTypes []string) {
	fmt.Printf("Datalake report for data source types %+v\n", dataSourceTypes)
	fmt.Printf("Datalake report for projects %+v\n", roots)
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
	gDbg = os.Getenv("DBG") != ""
	gReport = os.Getenv("REPORT")
	if gReport == "" {
		fatal("REPORT must be set")
	}
	gESURL = os.Getenv("ES_URL")
	if gESURL == "" {
		fatal("ES_URL must be set")
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
	default:
		fatal("unknown report type: " + gReport)
	}
	sDSes := strings.TrimSpace(os.Getenv("DATASOURCES"))
	if sDSes != "" {
		ary := strings.Split(sDSes, ",")
		gDatasource = make(map[string]struct{})
		for _, ds := range ary {
			gDatasource[strings.TrimSpace(ds)] = struct{}{}
		}
	}
	gAll = make(map[string]struct{})
}

func main() {
	setupEnvs()
	setupSHDB()
	switch gReport {
	case "org":
		genOrgReport(getSlugRoots())
	case "datalake":
		genDatalakeReport(getSlugRoots())
	}
}
