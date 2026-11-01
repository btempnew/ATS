package main

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/strelov1/freehire/internal/ingest/sources"
)

type preciseSalary struct {
	Min      *float64 `json:"min"`
	Max      *float64 `json:"max"`
	Currency string   `json:"currency"`
	Period   string   `json:"period"`
}

// FreeHire's Job schema rounds salary to integers. Retain the API's original
// decimal wage in our output without changing its adapters or issuing more requests.
func (c *locationClient) GetJSON(ctx context.Context, raw string, value any) error {
	u, _ := url.Parse(raw)
	if u == nil || (u.Hostname() != "api.lever.co" && u.Hostname() != "api.eu.lever.co" && u.Hostname() != "api.ashbyhq.com") {
		return c.HTTPClient.GetJSON(ctx, raw, value)
	}
	var body json.RawMessage
	if err := c.HTTPClient.GetJSON(ctx, raw, &body); err != nil {
		return err
	}
	if err := json.Unmarshal(body, value); err != nil {
		return err
	}
	c.rememberSalaries(u.Hostname(), body)
	return nil
}

func (c *locationClient) rememberSalaries(host string, body []byte) {
	if host == "api.lever.co" || host == "api.eu.lever.co" {
		var jobs []struct {
			URL    string `json:"hostedUrl"`
			Salary *struct {
				Min      *float64 `json:"min"`
				Max      *float64 `json:"max"`
				Currency string   `json:"currency"`
				Interval string   `json:"interval"`
			} `json:"salaryRange"`
		}
		if json.Unmarshal(body, &jobs) != nil {
			return
		}
		for _, j := range jobs {
			if j.Salary == nil || j.URL == "" {
				continue
			}
			period := map[string]string{"per-year-salary": "year", "per-month-salary": "month", "per-hour-wage": "hour"}[j.Salary.Interval]
			if period != "" {
				c.salaries.Store(j.URL, preciseSalary{j.Salary.Min, j.Salary.Max, j.Salary.Currency, period})
			}
		}
		return
	}
	var response struct {
		Jobs []sources.AshbyPosting `json:"jobs"`
	}
	if json.Unmarshal(body, &response) != nil {
		return
	}
	for _, j := range response.Jobs {
		if j.Compensation == nil || j.JobURL == "" {
			continue
		}
	firstSalary:
		for _, tier := range j.Compensation.CompensationTiers {
			for _, component := range tier.Components {
				if component.CompensationType != "Salary" {
					continue
				}
				period := map[string]string{"1 YEAR": "year", "1 MONTH": "month", "1 DAY": "day", "1 HOUR": "hour"}[component.Interval]
				if period != "" {
					c.salaries.Store(j.JobURL, preciseSalary{component.MinValue, component.MaxValue, component.CurrencyCode, period})
				}
				break firstSalary
			}
		}
	}
}

func applyPreciseSalary(j *outputJob, c *locationClient) {
	value, ok := c.salaries.Load(j.URL)
	if !ok {
		return
	}
	s := value.(preciseSalary)
	if s.Min != nil && *s.Min <= 0 {
		s.Min = nil
	}
	if s.Max != nil && *s.Max <= 0 {
		s.Max = nil
	}
	if s.Min == nil && s.Max == nil {
		return
	}
	j.SalaryMin, j.SalaryMax, j.SalaryCurrency, j.SalaryPeriod = s.Min, s.Max, s.Currency, s.Period
	j.Structured["salary"] = s
}
