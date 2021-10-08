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
	gSubReport  map[string]struct{}
	gAll        map[string]struct{}
	gDt         time.Time
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
	createdAt   time.Time
	locAdded    int
	locDeleted  int
	filtered    bool
}

// Docs stats report item (only confluence)
// subrep
type datalakeDocReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	createdAt   time.Time
	actionType  string // page, new_page, comment, attachment
	filtered    bool
}

// PRs stats report item (github pull_request & gerrit)
// subrep
type datalakePRReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	createdAt   time.Time
	actionType  string // all possible doc types from github-issue (PRs only) and gerrit, also detecting approvals/rejections/merges
	filtered    bool
}

// Issues stats report item (github issue, Jira issue, Bugzilla(rest) issue)
// subrep
type datalakeIssueReportItem struct {
	docID       string
	identityID  string
	dataSource  string
	projectSlug string
	createdAt   time.Time
	actionType  string // all possible doc types from github-issue (Issue only), Jira & Bugzilla(rest)
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
		// to limit data processing while implementing
		/*
			if !strings.Contains(idx, "git") {
				continue
			}
		*/
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

func toYMDHMSDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

func applySlugMapping(slug string) (daName, sfName string, found bool) {
	slugs := []string{slug}
	if gDbg {
		defer func() {
			fmt.Printf("slug mapping: %s -> %+v -> %s,%s,%v\n", slug, slugs, daName, sfName, found)
		}()
	}
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
			daName = slg
			found = true
			return
		}
	}
	sfName = slug
	daName = slug
	return
}

// subrep
func datalakeLOCReportForRoot(root, projectSlug string, overrideProjectSlug bool) (locItems []datalakeLOCReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got LOC %s: %v\n", root, locItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-git*,-*-github,-*-github-issue,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select git_uuid, author_id, project_slug, metadata__enriched_on, lines_added, lines_removed from \"%s\" `+
			`where author_id is not null and type = 'commit' and (lines_added > 0 or lines_removed > 0)","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeLOCReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [f16103509162d3044c5882a2b0d4c4cf70c16cb0 7f6daae75c73b05795d1cffb2f6642d51ad94ab5 accord/accord-concerto 2021-08-11T12:16:55.000Z 1 1]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			createdAt, _ := timeParseES(row[3].(string))
			fLOCAdded, _ := row[4].(float64)
			locAdded := int(fLOCAdded)
			fLOCDeleted, _ := row[5].(float64)
			locDeleted := int(fLOCDeleted)
			item := datalakeLOCReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "git",
				projectSlug: pSlug,
				createdAt:   createdAt,
				locAdded:    locAdded,
				locDeleted:  locDeleted,
				filtered:    false,
			}
			locItems = append(locItems, item)
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
			fmt.Printf("datalakeLOCReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeGithubPRReportForRoot(root, projectSlug string, overrideProjectSlug bool) (prItems []datalakePRReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got github-pr %s: %v\n", root, prItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-github-issue*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select id, author_id, project_slug, metadata__enriched_on, type, state, merged_by_data_id from \"%s\" `+
			`where author_id is not null and is_github_pull_request = 1","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeGitHubPRReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [ffd3f84758dbdd21df9a66b09a96df1ae47db0f3 2ca78b1cc8c9bb81152d3495a5aa8058fba5801c academy-software-foundation/opencolorio 2021-09-16T06:33:13.461Z pull_request closed 5675ad72e0958195400713bc2430e77315cbedf9]
			// [ffd3f84758dbdd21df9a66b09a96df1ae47db0f3 5675ad72e0958195400713bc2430e77315cbedf9 academy-software-foundation/opencolorio 2021-09-16T06:33:13.461Z pull_request_review APPROVED <nil>]
			// [293c9ccfe121c0b895c9b7cf0ce20aa11f142aca 2ab3287aa9894fbb6b395e544fc579ba3126c8c7 academy-software-foundation/opencolorio 2021-10-04T23:32:45.981Z issue_comment <nil> <nil>]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
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
			if identityID == mergeIdentityID {
				actionType = "PR merged"
			}
			item := datalakePRReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "github/issue",
				projectSlug: pSlug,
				createdAt:   createdAt,
				actionType:  actionType,
				filtered:    false,
			}
			prItems = append(prItems, item)
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
			fmt.Printf("datalakeGitHubPRReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeGerritReviewReportForRoot(root, projectSlug string, overrideProjectSlug bool) (prItems []datalakePRReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got gerrit-review %s: %v\n", root, prItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-gerrit*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select id, author_id, project_slug, metadata__enriched_on, type, approval_value from \"%s\" `+
			`where author_id is not null","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeGerritReviewReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [78843539ad1642c4ab74ae6c19772d1a40bf951d a006613a2aefee76acceb8f5fb6867f49c32ebe1 o-ran/shared 2021-09-07T05:56:36.913Z approval 1]
			// [03c753252a0c27bd890abf272c93e61bb1e28c31 6183ef1a4ac9e5403ff6bf13a1508654def1bb09 o-ran/shared 2021-09-07T05:56:31.998Z patchset <nil>]
			// [235bff88695c1e049b904edc9b7744357acf11ac 81e04dfee8603594a86b780010f7669650d7f076 lfn/tungsten-fabric 2021-09-07T05:37:36.574Z comment <nil>]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
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
			item := datalakePRReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "gerrit",
				projectSlug: pSlug,
				createdAt:   createdAt,
				actionType:  actionType,
				filtered:    false,
			}
			prItems = append(prItems, item)
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
			fmt.Printf("datalakeGerritReviewReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeGithubIssueReportForRoot(root, projectSlug string, overrideProjectSlug bool) (issueItems []datalakeIssueReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got github-issue %s: %v\n", root, issueItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-github-issue*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select id, author_id, project_slug, metadata__enriched_on, type from \"%s\" `+
			`where author_id is not null and is_github_issue = 1","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeGitHubIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [bdf57807ad938a88ba699da2bdf49bf851884dc9 d21d4c49184b31550fd6526863fb64894f437e64 hyperledger/sawtooth 2021-09-07T14:49:35.631Z issue_assignee]
			// [9ff1f7e20274861e9415a5af8e7f330055a658f8 342fc492a4e5fd634c527bb09b3c079f0737e3cd ojsf/ojsf-nodejs 2021-09-07T06:22:45.628Z issue_comment]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "github/issue",
				projectSlug: pSlug,
				createdAt:   createdAt,
				actionType:  actionType,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
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
			fmt.Printf("datalakeGitHubIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeJiraIssueReportForRoot(root, projectSlug string, overrideProjectSlug bool) (issueItems []datalakeIssueReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got jira issue %s: %v\n", root, issueItems)
		}()
	}
	// can also get status and/or status_category_key
	pattern := jsonEscape("sds-" + root + "-jira*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select id, author_id, project_slug, metadata__enriched_on, type from \"%s\" `+
			`where author_id is not null","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeJiraIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [3fc01f224df4430fa86a03aa7a7d581210e58f64 30f4c2f64cf70d52ed96b4e2fd7f46ae3b59c728 lfn/pnda 2021-09-07T05:42:55.185Z issue]
			// [053c26541dd50c096e316a38feded0cb1225c051 e439d8d016a641d90ca408b9014afc675d55ad50 hyperledger/cello 2021-09-07T13:57:20.029Z comment]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			// status, _ := row[5].(string)
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "jira",
				projectSlug: pSlug,
				createdAt:   createdAt,
				actionType:  "jira_" + actionType,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
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
			fmt.Printf("datalakeJiraIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeBugzillaIssueReportForRoot(root, projectSlug string, overrideProjectSlug, rest bool) (issueItems []datalakeIssueReportItem) {
	ds := "bugzilla"
	if rest {
		ds += "rest"
	}
	if gDbg {
		defer func() {
			fmt.Printf("got %s issue %s: %v\n", ds, root, issueItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-" + ds + "*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	// can also get status and/or status_category_key
	// TODO: is uuid an unique document key in bugzilla?
	data := fmt.Sprintf(
		`{"query":"select uuid, author_id, metadata__enriched_on, status from \"%s\" `+
			`where author_id is not null","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeBugzillaIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	processResults := func() {
		for _, row := range result.Rows {
			// [5be4b29396a72501206f561082f2f87f696bc9ef 29dbe15b5c802d2e32e9d02a76798815660005b8 2021-10-04T19:52:27.208Z RESOLVED]
			// [16e6334b1385f7ff83dac3cd322dfd3ecfdc8f5e 77f6e01aaac4e573205fb8e8097b91b253f76067 2021-10-04T19:52:29.152Z UNCONFIRMED]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			createdAt, _ := timeParseES(row[2].(string))
			status, _ := row[3].(string)
			item := datalakeIssueReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  ds,
				projectSlug: projectSlug,
				createdAt:   createdAt,
				actionType:  "bugzilla:" + status,
				filtered:    false,
			}
			issueItems = append(issueItems, item)
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
			fmt.Printf("datalakeBugzillaIssueReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	return
}

// subrep
func datalakeDocReportForRoot(root, projectSlug string, overrideProjectSlug bool) (docItems []datalakeDocReportItem) {
	if gDbg {
		defer func() {
			fmt.Printf("got docs %s: %v\n", root, docItems)
		}()
	}
	pattern := jsonEscape("sds-" + root + "-confluence*,-*-raw,-*-for-merge,-*-cache,-*-converted,-*-temp,-*-last-action-date-cache")
	method := "POST"
	data := fmt.Sprintf(
		`{"query":"select uuid, author_id, project_slug, metadata__enriched_on, type from \"%s\" `+
			`where author_id is not null","fetch_size":%d}`,
		pattern,
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
			fmt.Printf("datalakeDocReportForRoot: error for %s: %v\n", pattern, result.Error)
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
		for _, row := range result.Rows {
			// [cff117ac4b479e31473727cd775f7e96746e7e7e 9e7702dd25aac483d9104b4ba93fa1b73c751083 academy-software-foundation/academy-software-foundation-common 2021-10-01T05:14:53.931Z page]
			documentID, _ := row[0].(string)
			identityID, _ := row[1].(string)
			if !overrideProjectSlug {
				pSlug, _ = row[2].(string)
			}
			createdAt, _ := timeParseES(row[3].(string))
			actionType, _ := row[4].(string)
			item := datalakeDocReportItem{
				docID:       documentID,
				identityID:  identityID,
				dataSource:  "confluence",
				projectSlug: pSlug,
				createdAt:   createdAt,
				actionType:  actionType,
				filtered:    false,
			}
			docItems = append(docItems, item)
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
			fmt.Printf("datalakeDocReportForRoot: error for %s: %v\n", pattern, result.Error)
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
	daName, sfName, found := applySlugMapping(root)
	if gDbg {
		fmt.Printf("running for: %s %v -> %s,%s,%v\n", root, dataSourceTypes, daName, sfName, found)
	}
	if bLOC {
		itemData.locItems = datalakeLOCReportForRoot(root, daName, found)
	}
	if bDocs {
		itemData.docItems = datalakeDocReportForRoot(root, daName, found)
	}
	if bPRs {
		itemData.prItems = datalakeGithubPRReportForRoot(root, daName, found)
		prItems := datalakeGerritReviewReportForRoot(root, daName, found)
		if len(prItems) > 0 {
			itemData.prItems = append(itemData.prItems, prItems...)
		}
	}
	if bIssues {
		itemData.issueItems = datalakeGithubIssueReportForRoot(root, daName, found)
		issueItems := datalakeJiraIssueReportForRoot(root, daName, found)
		if len(issueItems) > 0 {
			itemData.issueItems = append(itemData.issueItems, issueItems...)
		}
		issueItems = datalakeBugzillaIssueReportForRoot(root, daName, found, false)
		if len(issueItems) > 0 {
			itemData.issueItems = append(itemData.issueItems, issueItems...)
		}
		issueItems = datalakeBugzillaIssueReportForRoot(root, daName, found, true)
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
	_, sfName, _ := applySlugMapping(root)
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
			toYMDHMSDate(item.createdAt),
			strconv.Itoa(item.locAdded),
			strconv.Itoa(item.locDeleted),
			filtered,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][4] > rows[j][4]
		},
	)
	var writer *csv.Writer
	file, err := os.Create("datalake_loc.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"ES Document Id", "Identity Id", "Datasource", "Insights Project Slug", "Created At", "LOC Added", "LOC Deleted", "Filtered"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
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
	case "GitHub PR approved", "GitHub PR changes requested", "GitHub PR dismissed", "GitHub PR review comment", "PR merged", "Gerrit review rejected", "Gerrit review approved":
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
			toYMDHMSDate(item.createdAt),
			toPRType(item.actionType),
			filtered,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][4] > rows[j][4]
		},
	)
	var writer *csv.Writer
	file, err := os.Create("datalake_prs.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"ES Document Id", "Identity Id", "Datasource", "Insights Project Slug", "Created At", "Type", "Filtered"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
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
			toYMDHMSDate(item.createdAt),
			toIssueType(item.actionType),
			filtered,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][4] > rows[j][4]
		},
	)
	var writer *csv.Writer
	file, err := os.Create("datalake_issues.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"ES Document Id", "Identity Id", "Datasource", "Insights Project Slug", "Created At", "Type", "Filtered"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
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
	default:
		return "?"
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
			toYMDHMSDate(item.createdAt),
			toDocType(item.actionType),
			filtered,
		}
		rows = append(rows, row)
	}
	sort.Slice(
		rows,
		func(i, j int) bool {
			return rows[i][4] > rows[j][4]
		},
	)
	var writer *csv.Writer
	file, err := os.Create("datalake_docs.csv")
	fatalError(err)
	defer func() { _ = file.Close() }()
	writer = csv.NewWriter(file)
	defer writer.Flush()
	fatalError(writer.Write([]string{"ES Document Id", "Identity Id", "Datasource", "Insights Project Slug", "Created At", "Type", "Filtered"}))
	for _, row := range rows {
		fatalError(writer.Write(row))
	}
}

func genOrgReport(roots, dataSourceTypes []string) {
	thrN := runtime.NumCPU()
	// thrN = 1
	// if len(roots) > 20 {
	//	 roots = roots[:20]
	// }
	if thrN > 20 {
		thrN = 20
	}
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
			fmt.Printf("LOC data dedup: %d -> %d\n", len(report.locItems), len(locItems))
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
			fmt.Printf("Docs data dedup: %d -> %d\n", len(report.docItems), len(docItems))
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
			fmt.Printf("PR data dedup: %d -> %d\n", len(report.prItems), len(prItems))
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
			fmt.Printf("Issue data dedup: %d -> %d\n", len(report.issueItems), len(issueItems))
			report.issueItems = issueItems
		} else {
			fmt.Printf("no duplicate elements found, issue items: %d\n", len(report.issueItems))
		}
	}
}

// subrep
func filterDatalakeReport(report *datalakeReport, identityIDs map[string]struct{}, bLOC, bDocs, bPRs, bIssues bool) {
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
			fmt.Printf("filtered %d/%d loc items\n", locFiltered, len(report.locItems))
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
			fmt.Printf("filtered %d/%d docs items\n", docFiltered, len(report.docItems))
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
			fmt.Printf("filtered %d/%d PR items\n", prFiltered, len(report.prItems))
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
			fmt.Printf("filtered %d/%d issue items\n", issueFiltered, len(report.issueItems))
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
	// thrN = 1
	// if len(roots) > 20 {
	//	 roots = roots[:20]
	// }
	if thrN > 20 {
		thrN = 20
	}
	runtime.GOMAXPROCS(thrN)
	chIdentIDs := make(chan map[string]struct{})
	go getAffiliatedNonBotIdentities(chIdentIDs)
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
	identityIDs := <-chIdentIDs
	fmt.Printf("%d non-bot identities having at least one enrollment present in SH DB\n", len(identityIDs))
	filterDatalakeReport(&report, identityIDs, bLOC, bDocs, bPRs, bIssues)
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
	}
}
