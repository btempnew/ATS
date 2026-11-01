package main

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/strelov1/freehire/internal/ingest/sources"
	"golang.org/x/net/html"
	"golang.org/x/time/rate"
)

// The upstream SuccessFactors adapter intentionally leaves Location empty. Capture
// the page's explicit workplace label while its existing detail request is read,
// so the US-only filter does not discard every posting or infer geography from prose.
type locationClient struct {
	sources.HTTPClient
	hosts     map[string]bool
	locations sync.Map
	salaries  sync.Map
	limiter   *rate.Limiter
}

func textOf(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data + " "
	}
	var b strings.Builder
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		b.WriteString(textOf(child))
	}
	return b.String()
}

func explicitWorkplace(root *html.Node) string {
	var structured string
	var scan func(*html.Node)
	scan = func(n *html.Node) {
		if structured != "" {
			return
		}
		for _, attr := range n.Attr {
			if attr.Key == "itemprop" && attr.Val == "jobLocation" {
				parts := map[string]string{}
				var address func(*html.Node)
				address = func(node *html.Node) {
					prop, content := "", ""
					for _, a := range node.Attr {
						if a.Key == "itemprop" {
							prop = a.Val
						}
						if a.Key == "content" {
							content = a.Val
						}
					}
					if prop == "addressLocality" || prop == "addressRegion" || prop == "addressCountry" {
						if content == "" {
							content = strings.Join(strings.Fields(textOf(node)), " ")
						}
						parts[prop] = content
					}
					for child := node.FirstChild; child != nil; child = child.NextSibling {
						address(child)
					}
				}
				address(n)
				var values []string
				for _, field := range []string{"addressLocality", "addressRegion", "addressCountry"} {
					if parts[field] != "" {
						values = append(values, parts[field])
					}
				}
				structured = strings.Join(values, ", ")
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			scan(child)
		}
	}
	scan(root)
	if structured != "" {
		return structured
	}
	var found string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if found != "" {
			return
		}
		if n.Type == html.ElementNode && n.Data == "span" {
			label := strings.ToLower(strings.TrimSpace(textOf(n)))
			isLabel := false
			for _, attr := range n.Attr {
				if attr.Key == "class" && strings.Contains(" "+attr.Val+" ", " jobGeoLocation ") {
					found = strings.Join(strings.Fields(textOf(n)), " ")
					return
				}
				if attr.Key == "class" && strings.Contains(" "+attr.Val+" ", " joblayouttoken-label ") {
					isLabel = true
				}
			}
			if isLabel && (label == "job location:" || label == "location:") {
				for sibling := n.NextSibling; sibling != nil; sibling = sibling.NextSibling {
					if sibling.Type == html.ElementNode {
						found = strings.Join(strings.Fields(textOf(sibling)), " ")
						return
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return found
}

func (c *locationClient) GetHTML(ctx context.Context, raw string) (*html.Node, error) {
	u, _ := url.Parse(raw)
	tracked := u != nil && c.hosts[u.Hostname()]
	if tracked {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	fetchURL := raw
	if u != nil && u.Hostname() == "jobs.jobvite.com" {
		// Jobvite's public no-layout view keeps listing/detail HTML on the ATS
		// instead of redirecting to an employer's JS-only embedding page.
		query := u.Query()
		query.Set("nl", "1")
		u.RawQuery = query.Encode()
		fetchURL = u.String()
	}
	root, err := c.HTTPClient.GetHTML(ctx, fetchURL)
	if err == nil && tracked {
		if place := explicitWorkplace(root); place != "" {
			c.locations.Store(raw, place)
		}
	}
	return root, err
}

type geographicSource struct {
	sources.Source
	client *locationClient
}

func (s geographicSource) Fetch(ctx context.Context, e sources.CompanyEntry) ([]sources.Job, error) {
	jobs, err := s.Source.Fetch(ctx, e)
	for i := range jobs {
		if jobs[i].Location == "" {
			if place, ok := s.client.locations.Load(jobs[i].URL); ok {
				jobs[i].Location = place.(string)
			}
		}
	}
	return jobs, err
}
