// Getworkin ingestion bridge. ATS adapters and HTTP transport are reused from
// FreeHire (MIT), pinned by ../upstream.json; no FreeHire database is required.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/strelov1/freehire/internal/dict/location"
	"github.com/strelov1/freehire/internal/ingest/atsboard"
	"github.com/strelov1/freehire/internal/ingest/sources"
	"golang.org/x/time/rate"
)

var supported = map[string]bool{
	"greenhouse": true, "lever": true, "ashby": true, "workday": true,
	"smartrecruiters": true, "workable": true, "bamboohr": true, "icims": true,
	"jobvite": true, "teamtailor": true, "oracle": true, "successfactors": true,
	"adp": true, "paylocity": true, "dayforce": true, "taleo": true,
}

var upstreamCommit = "development"

var labels = map[string]string{
	"greenhouse": "Greenhouse", "lever": "Lever", "ashby": "Ashby", "workday": "Workday",
	"smartrecruiters": "SmartRecruiters", "workable": "Workable", "bamboohr": "BambooHR",
	"icims": "iCIMS", "jobvite": "Jobvite", "teamtailor": "Teamtailor", "oracle": "Oracle",
	"successfactors": "SuccessFactors", "adp": "Adp", "paylocity": "Paylocity", "dayforce": "Dayforce",
	"taleo": "Taleo",
}

type board struct {
	Provider   string `json:"provider"`
	Board      string `json:"board"`
	Company    string `json:"company"`
	Region     string `json:"region,omitempty"`
	Status     string `json:"status,omitempty"`
	Provenance string `json:"provenance,omitempty"`
}

func (b board) entry() sources.CompanyEntry {
	return sources.CompanyEntry{Provider: b.Provider, Board: b.Board, Company: b.Company, Region: b.Region}
}

func (b board) key() string {
	k, _ := sources.BoardDedupeKey(b.entry())
	return k
}

func digest(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

type cachedJob struct {
	Job        sources.Job `json:"job"`
	HydratedAt time.Time   `json:"hydrated_at"`
}

type boardState struct {
	Identity  string               `json:"identity"`
	UpdatedAt time.Time            `json:"updated_at"`
	Jobs      map[string]cachedJob `json:"jobs"`
}

type boardResult struct {
	Provider        string  `json:"provider"`
	Board           string  `json:"board"`
	Region          string  `json:"region,omitempty"`
	Company         string  `json:"company"`
	Status          string  `json:"status"`
	Listed          int     `json:"listed"`
	USJobs          int     `json:"us_jobs"`
	UnknownCountry  int     `json:"unknown_country"`
	OtherCountry    int     `json:"other_country"`
	Unreadable      int     `json:"unreadable"`
	Invalid         int     `json:"invalid"`
	Cached          int     `json:"cached"`
	ListingComplete bool    `json:"listing_complete"`
	Error           string  `json:"error,omitempty"`
	ElapsedSeconds  float64 `json:"elapsed_seconds"`
}

type report struct {
	StartedAt      time.Time     `json:"started_at"`
	FinishedAt     time.Time     `json:"finished_at"`
	UpstreamCommit string        `json:"upstream_commit"`
	Boards         []boardResult `json:"boards"`
	Jobs           int           `json:"jobs"`
	Duplicates     int           `json:"duplicates"`
	FailedBoards   int           `json:"failed_boards"`
}

type outputJob struct {
	ExternalID      string         `json:"external_id"`
	BoardID         string         `json:"board_id"`
	SourceBoard     string         `json:"source_board"`
	Title           string         `json:"job_title"`
	Company         string         `json:"company"`
	Location        string         `json:"location"`
	Countries       []string       `json:"countries"`
	CountryEvidence string         `json:"country_evidence"`
	URL             string         `json:"job_url"`
	ApplyURL        string         `json:"apply_url"`
	Description     string         `json:"description"`
	Remote          bool           `json:"is_remote"`
	WorkMode        string         `json:"work_mode,omitempty"`
	EmploymentType  string         `json:"job_type,omitempty"`
	Skills          []string       `json:"skills,omitempty"`
	PostedAt        *time.Time     `json:"posted_at,omitempty"`
	SalaryMin       *float64       `json:"salary_min,omitempty"`
	SalaryMax       *float64       `json:"salary_max,omitempty"`
	SalaryCurrency  string         `json:"salary_currency,omitempty"`
	SalaryPeriod    string         `json:"salary_period,omitempty"`
	Structured      map[string]any `json:"structured_fields"`
	AlsoOn          []string       `json:"also_on,omitempty"`
}

// Explicit posting geography wins. A bare "Remote" is never assumed to mean USA.
func countries(j sources.Job) ([]string, string) {
	if len(j.Countries) > 0 {
		var out []string
		for _, c := range j.Countries {
			if c = location.NormalizeCountry(c); c != "" {
				out = append(out, c)
			}
		}
		return out, "structured"
	}
	// Some boards qualify a country with a colon: "Remote - US: All locations".
	return location.Parse(strings.ReplaceAll(j.Location, ":", ",")).Countries, "location"
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func mapJob(b board, j sources.Job, cc []string, evidence string) outputJob {
	sf := map[string]any{}
	if j.WorkMode != "" {
		sf["work_mode"] = j.WorkMode
	}
	if j.EmploymentType != "" {
		sf["job_type"] = j.EmploymentType
	}
	if j.ExperienceYearsMin != nil {
		sf["experience_min"] = *j.ExperienceYearsMin
	}
	if j.SalaryMin != nil || j.SalaryMax != nil {
		sf["salary"] = map[string]any{"min": j.SalaryMin, "max": j.SalaryMax, "currency": j.SalaryCurrency, "period": j.SalaryPeriod}
	}
	return outputJob{ExternalID: j.ExternalID, BoardID: b.Board, SourceBoard: labels[b.Provider],
		Title: j.Title, Company: j.Company, Location: j.Location, Countries: cc, CountryEvidence: evidence,
		URL: j.URL, ApplyURL: j.URL, Description: j.Description, Remote: j.Remote, WorkMode: j.WorkMode,
		EmploymentType: j.EmploymentType, Skills: j.Skills, PostedAt: j.PostedAt,
		SalaryMin: floatPart(j.SalaryMin), SalaryMax: floatPart(j.SalaryMax), SalaryCurrency: j.SalaryCurrency,
		SalaryPeriod: j.SalaryPeriod, Structured: sf}
}

func usable(j sources.Job) bool {
	u, err := url.Parse(j.URL)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" &&
		strings.TrimSpace(j.ExternalID) != "" && strings.TrimSpace(j.Title) != "" &&
		strings.TrimSpace(j.Company) != "" && strings.TrimSpace(j.Description) != ""
}

func floatPart(value *int) *float64 {
	if value == nil {
		return nil
	}
	f := float64(*value)
	return &f
}

func atomicJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".ingest-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if err = json.NewEncoder(f).Encode(value); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func crawl(ctx context.Context, b board, src sources.Source, stateDir string, ttl time.Duration, fullListing bool) ([]outputJob, boardResult) {
	started := time.Now()
	r := boardResult{Provider: b.Provider, Board: b.Board, Region: b.Region, Company: b.Company, Status: "ok"}
	finish := func() { r.ElapsedSeconds = time.Since(started).Seconds() }
	path := filepath.Join(stateDir, digest(b.key())+".json")
	cache := boardState{Identity: b.key(), Jobs: map[string]cachedJob{}}
	if data, err := os.ReadFile(path); err == nil {
		if err = json.Unmarshal(data, &cache); err != nil || cache.Identity != b.key() || cache.Jobs == nil {
			r.Status, r.Error = "failed", "invalid board cache; move it aside and retry"
			finish()
			return nil, r
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		r.Status, r.Error = "failed", err.Error()
		finish()
		return nil, r
	}
	var raw []sources.Job
	var err error
	if h, ok := src.(sources.HydratingSource); ok {
		raw, err = h.FetchNew(ctx, b.entry(), func(id string) bool {
			old, exists := cache.Jobs[id]
			return exists && usable(old.Job) && time.Since(old.HydratedAt) < ttl
		})
	} else {
		raw, err = src.Fetch(ctx, b.entry())
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		r.Status, r.Error = "failed", err.Error()
		if len(raw) == 0 {
			finish()
			return nil, r
		}
		r.Status = "partial"
	}
	out := []outputJob{}
	seen := map[string]bool{}
	for _, j := range raw {
		if seen[j.ExternalID] {
			continue
		}
		seen[j.ExternalID] = true
		r.Listed++
		if j.Removed {
			continue
		}
		if j.Unreadable {
			r.Unreadable++
			continue
		}
		wasRefresh := j.SeenRefresh
		if j.SeenRefresh {
			old, ok := cache.Jobs[j.ExternalID]
			if !ok {
				r.Unreadable++
				continue
			}
			j = old.Job
			r.Cached++
		}
		if !usable(j) {
			r.Invalid++
			continue
		}
		if !wasRefresh {
			cache.Jobs[j.ExternalID] = cachedJob{Job: j, HydratedAt: time.Now().UTC()}
		}
		cc, evidence := countries(j)
		if !contains(cc, "us") {
			if len(cc) == 0 {
				r.UnknownCountry++
			} else {
				r.OtherCountry++
			}
			continue
		}
		out = append(out, mapJob(b, j, cc, evidence))
		r.USJobs++
	}
	// Keep old cache entries after partial crawls. No expiry decisions are made here.
	r.ListingComplete = err == nil && fullListing && r.Unreadable == 0 && r.Invalid == 0
	cache.UpdatedAt = time.Now().UTC()
	if r.Unreadable > 0 || r.Invalid > 0 {
		r.Status = "partial"
	}
	if len(raw) == 0 {
		r.Status = "empty"
	}
	if err := atomicJSON(path, cache); err != nil {
		r.Status, r.Error = "failed", err.Error()
	}
	finish()
	return out, r
}

// Collapse exact destination duplicates only. Similar title/location is insufficient
// evidence that two employer requisitions are the same vacancy.
func dedupe(jobs []outputJob) ([]outputJob, int) {
	out := []outputJob{}
	byURL := map[string]int{}
	duplicates := 0
	for _, j := range jobs {
		u, _ := url.Parse(j.URL)
		u.Fragment = ""
		key := u.String()
		if i, ok := byURL[key]; ok {
			duplicates++
			out[i].AlsoOn = append(out[i].AlsoOn, j.SourceBoard+":"+j.BoardID+":"+j.ExternalID)
			continue
		}
		byURL[key] = len(out)
		out = append(out, j)
	}
	return out, duplicates
}

type candidate struct {
	URL        string `json:"url"`
	Company    string `json:"company"`
	Provenance string `json:"provenance"`
}

func detectCandidates(items []candidate) []board {
	out := []board{}
	seen := map[string]bool{}
	for _, item := range items {
		p, id, canonical, ok := atsboard.Recognize(item.URL)
		if !ok || !supported[p] {
			continue
		}
		region := ""
		if u, err := url.Parse(canonical); err == nil && p == "lever" && u.Hostname() == "jobs.eu.lever.co" {
			region = "eu"
		}
		company := strings.TrimSpace(item.Company)
		if company == "" {
			company = id
		}
		b := board{Provider: p, Board: id, Region: region, Company: company, Status: "pending", Provenance: item.Provenance}
		if seen[b.key()] {
			continue
		}
		seen[b.key()] = true
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

func run() error {
	catalog := flag.String("catalog", "", "JSON array of provider/board/company entries")
	detect := flag.String("detect-urls", "", "classify a JSON array of URL/company/provenance candidates without crawling")
	output := flag.String("output", "jobs.json", "US jobs JSON output")
	stateDir := flag.String("state", "state", "persistent per-board detail cache")
	reportPath := flag.String("report", "report.json", "per-board validation report")
	providerFlag := flag.String("providers", "", "optional comma-separated provider subset")
	workers := flag.Int("workers", 2, "boards crawled concurrently")
	maxBoards := flag.Int("max-boards", 0, "maximum boards per provider, 0 = all")
	boardTimeout := flag.Duration("board-timeout", 5*time.Minute, "deadline per board")
	ttl := flag.Duration("detail-ttl", 7*24*time.Hour, "refresh cached detail after this period")
	flag.Parse()
	if *detect != "" {
		data, err := os.ReadFile(*detect)
		if err != nil {
			return err
		}
		var items []candidate
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		return atomicJSON(*output, detectCandidates(items))
	}
	if *catalog == "" || *workers < 1 || *maxBoards < 0 || *boardTimeout <= 0 || *ttl <= 0 {
		return fmt.Errorf("valid catalog, workers, max-boards, timeout and TTL are required")
	}
	data, err := os.ReadFile(*catalog)
	if err != nil {
		return err
	}
	var boards []board
	if err = json.Unmarshal(data, &boards); err != nil {
		return err
	}
	selected := map[string]bool{}
	if *providerFlag != "" {
		for _, p := range strings.Split(*providerFlag, ",") {
			p = strings.TrimSpace(p)
			if !supported[p] {
				return fmt.Errorf("unsupported provider %q", p)
			}
			selected[p] = true
		}
	}
	sfHosts := map[string]bool{}
	for _, b := range boards {
		if b.Provider == "successfactors" {
			sfHosts[b.Board] = true
		}
	}
	client := &locationClient{HTTPClient: sources.NewClient(), hosts: sfHosts, limiter: rate.NewLimiter(2, 1)}
	registry := sources.All(client)
	full := sources.FullBoardListingProviders(registry)
	registry["successfactors"] = geographicSource{Source: registry["successfactors"], client: client}
	seenBoards, counts := map[string]bool{}, map[string]int{}
	var work []board
	for _, b := range boards {
		if !supported[b.Provider] {
			return fmt.Errorf("unsupported provider %q", b.Provider)
		}
		if b.Status == "retired" || b.Status == "disabled" || b.Status == "rejected" {
			continue
		}
		if b.Status != "" && b.Status != "pending" && b.Status != "active" {
			return fmt.Errorf("unknown board status %q", b.Status)
		}
		if len(selected) > 0 && !selected[b.Provider] {
			continue
		}
		if err := (sources.Config{Sources: []sources.CompanyEntry{b.entry()}}).Validate(registry); err != nil {
			return err
		}
		if seenBoards[b.key()] || (*maxBoards > 0 && counts[b.Provider] >= *maxBoards) {
			continue
		}
		seenBoards[b.key()] = true
		counts[b.Provider]++
		work = append(work, b)
	}
	if len(work) == 0 {
		return fmt.Errorf("catalog contains no eligible boards")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := report{StartedAt: time.Now().UTC(), UpstreamCommit: upstreamCommit, Boards: []boardResult{}}
	all := []outputJob{}
	var mu sync.Mutex
	queue := make(chan board)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range queue {
				bctx, cancel := context.WithTimeout(ctx, *boardTimeout)
				jobs, result := crawl(bctx, b, registry[b.Provider], *stateDir, *ttl, full[b.Provider])
				cancel()
				for i := range jobs {
					applyPreciseSalary(&jobs[i], client)
				}
				mu.Lock()
				all = append(all, jobs...)
				r.Boards = append(r.Boards, result)
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "%s %s: %s, US=%d, listed=%d\n", b.Provider, b.Board, result.Status, result.USJobs, result.Listed)
			}
		}()
	}
enqueue:
	for _, b := range work {
		select {
		case queue <- b:
		case <-ctx.Done():
			break enqueue
		}
	}
	close(queue)
	wg.Wait()
	sort.Slice(r.Boards, func(i, j int) bool {
		return r.Boards[i].Provider+r.Boards[i].Board < r.Boards[j].Provider+r.Boards[j].Board
	})
	sort.Slice(all, func(i, j int) bool {
		return all[i].SourceBoard+all[i].BoardID+all[i].ExternalID < all[j].SourceBoard+all[j].BoardID+all[j].ExternalID
	})
	all, r.Duplicates = dedupe(all)
	r.Jobs = len(all)
	r.FinishedAt = time.Now().UTC()
	for _, b := range r.Boards {
		if b.Status == "failed" || b.Status == "partial" {
			r.FailedBoards++
		}
	}
	if err := atomicJSON(*output, all); err != nil {
		return err
	}
	if err := atomicJSON(*reportPath, r); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if r.FailedBoards > 0 {
		return fmt.Errorf("%d boards failed or were partial; inspect report (successful jobs were saved)", r.FailedBoards)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
