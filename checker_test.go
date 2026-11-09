package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHTTPCheckerCheckOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	checker := NewHTTPChecker(2 * time.Second)
	result := checker.Check(server.URL)

	if result.Err != nil {
		t.Fatalf("очікували успішну перевірку, отримали помилку: %v", result.Err)
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("очікували статус 200, отримали %d", result.StatusCode)
	}
	if result.Duration <= 0 {
		t.Fatal("тривалість запиту має бути більшою за нуль")
	}
}

func TestHTTPCheckerCheckNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	checker := NewHTTPChecker(2 * time.Second)
	result := checker.Check(server.URL)

	if result.Err != nil {
		t.Fatalf("очікували HTTP-відповідь, отримали помилку: %v", result.Err)
	}
	if result.StatusCode != http.StatusNotFound {
		t.Fatalf("очікували статус 404, отримали %d", result.StatusCode)
	}
}

func TestHTTPCheckerCheckError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serverURL := server.URL
	server.Close()

	checker := NewHTTPChecker(500 * time.Millisecond)
	result := checker.Check(serverURL)

	if result.Err == nil {
		t.Fatal("очікували помилку з'єднання, але її немає")
	}
}

func TestStatsConcurrent(t *testing.T) {
	var stats Stats
	var waitGroup sync.WaitGroup

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			switch {
			case index%4 == 0:
				stats.Add(CheckResult{URL: "https://example.com", StatusCode: http.StatusOK})
			case index%4 == 1:
				stats.Add(CheckResult{URL: "https://example.com", StatusCode: http.StatusNotFound})
			default:
				stats.Add(CheckResult{URL: "https://example.com", Err: errors.New("network error")})
			}
		}(i)
	}

	waitGroup.Wait()
	snapshot := stats.Snapshot()

	if snapshot.Total != 100 {
		t.Fatalf("очікували 100 перевірок, отримали %d", snapshot.Total)
	}
	if snapshot.OK200 != 25 {
		t.Fatalf("очікували 25 відповідей 200, отримали %d", snapshot.OK200)
	}
	if snapshot.HTTPError != 25 {
		t.Fatalf("очікували 25 HTTP-помилок, отримали %d", snapshot.HTTPError)
	}
	if snapshot.NetError != 50 {
		t.Fatalf("очікували 50 мережевих помилок, отримали %d", snapshot.NetError)
	}
}

type fakeChecker struct {
	delay  time.Duration
	status int
	err    error
}

func (f fakeChecker) Check(url string) CheckResult {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return CheckResult{
		URL:        url,
		StatusCode: f.status,
		Duration:   f.delay,
		Err:        f.err,
	}
}

func TestRunConcurrentMutex(t *testing.T) {
	urls := []string{"https://one.example", "https://two.example", "https://three.example"}
	checker := fakeChecker{delay: 5 * time.Millisecond, status: http.StatusOK}

	results, _, stats := runConcurrentMutex(checker, urls)

	if len(results) != len(urls) {
		t.Fatalf("очікували %d результатів, отримали %d", len(urls), len(results))
	}
	if stats.Total != len(urls) {
		t.Fatalf("очікували %d перевірок у статистиці, отримали %d", len(urls), stats.Total)
	}
}

func TestRunConcurrentChannels(t *testing.T) {
	urls := []string{"https://one.example", "https://two.example", "https://three.example", "https://four.example"}
	checker := fakeChecker{delay: 5 * time.Millisecond, status: http.StatusOK}

	results, _ := runConcurrentChannels(checker, urls)

	if len(results) != len(urls) {
		t.Fatalf("очікували %d результатів, отримали %d", len(urls), len(results))
	}
	for i := 1; i < len(results); i++ {
		if results[i-1].URL > results[i].URL {
			t.Fatal("результати мають бути відсортовані за URL")
		}
	}
}
