package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// CheckResult описує результат одного опитування адреси.
type CheckResult struct {
	URL        string
	StatusCode int
	Duration   time.Duration
	Err        error
}

// Checker абстрагує перевірку ресурсу. Завдяки інтерфейсу основна логіка
// не залежить від конкретної реалізації HTTP-клієнта.
type Checker interface {
	Check(url string) CheckResult
}

// HTTPChecker виконує реальні HTTP-запити з обмеженням часу очікування.
type HTTPChecker struct {
	client *http.Client
}

func NewHTTPChecker(timeout time.Duration) *HTTPChecker {
	return &HTTPChecker{
		client: &http.Client{Timeout: timeout},
	}
}

func (c *HTTPChecker) Check(url string) CheckResult {
	start := time.Now()
	result := CheckResult{URL: url}

	response, err := c.client.Get(url)
	result.Duration = time.Since(start)
	if err != nil {
		result.Err = err
		return result
	}
	defer response.Body.Close()

	// Дочитуємо тіло, щоб звільнити з'єднання для повторного використання.
	_, _ = io.Copy(io.Discard, response.Body)
	result.StatusCode = response.StatusCode
	return result
}

// Stats зберігає підсумкові лічильники. Поля змінюються з різних горутин,
// тому доступ до них захищений м'ютексом.
type Stats struct {
	mu        sync.Mutex
	Total     int
	OK200     int
	HTTPError int
	NetError  int
}

// StatsSnapshot - незмінна копія статистики для виведення на екран.
type StatsSnapshot struct {
	Total     int
	OK200     int
	HTTPError int
	NetError  int
}

func (s *Stats) Add(result CheckResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Total++
	switch {
	case result.Err != nil:
		s.NetError++
	case result.StatusCode == http.StatusOK:
		s.OK200++
	default:
		s.HTTPError++
	}
}

func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return StatsSnapshot{
		Total:     s.Total,
		OK200:     s.OK200,
		HTTPError: s.HTTPError,
		NetError:  s.NetError,
	}
}

func statsFromResults(results []CheckResult) StatsSnapshot {
	var stats StatsSnapshot
	for _, result := range results {
		stats.Total++
		switch {
		case result.Err != nil:
			stats.NetError++
		case result.StatusCode == http.StatusOK:
			stats.OK200++
		default:
			stats.HTTPError++
		}
	}
	return stats
}

func printResults(results []CheckResult) {
	for _, result := range results {
		if result.Err != nil {
			fmt.Printf("-> %s: Помилка (%v)\n", result.URL, result.Err)
			continue
		}
		fmt.Printf("-> %s: Status %d %s (час: %s)\n",
			result.URL,
			result.StatusCode,
			http.StatusText(result.StatusCode),
			result.Duration.Round(time.Millisecond),
		)
	}
}

func printStats(title string, stats StatsSnapshot, duration time.Duration) {
	fmt.Println("==========================================")
	fmt.Printf("ПІДСУМКОВИЙ ЗВІТ МОНІТОРИНГУ (%s):\n", title)
	fmt.Printf("- Усього перевірено адрес: %d\n", stats.Total)
	fmt.Printf("- Відповідей 200 OK: %d\n", stats.OK200)
	fmt.Printf("- Відповідей з іншим статусом: %d\n", stats.HTTPError)
	fmt.Printf("- Мережевих помилок: %d\n", stats.NetError)
	fmt.Printf("- Загальний час роботи: %s\n", duration.Round(time.Millisecond))
	fmt.Println("==========================================")
}

// runSequential виконує перевірки одну за одною в головному потоці.
func runSequential(checker Checker, urls []string) ([]CheckResult, time.Duration) {
	start := time.Now()
	results := make([]CheckResult, 0, len(urls))

	for _, url := range urls {
		results = append(results, checker.Check(url))
	}

	return results, time.Since(start)
}

// runConcurrentMutex запускає окрему горутину для кожної адреси.
// Спільний зріз результатів і статистика захищені sync.Mutex.
func runConcurrentMutex(checker Checker, urls []string) ([]CheckResult, time.Duration, StatsSnapshot) {
	start := time.Now()

	var waitGroup sync.WaitGroup
	var resultsMutex sync.Mutex
	results := make([]CheckResult, 0, len(urls))
	var stats Stats

	for _, url := range urls {
		url := url // фіксуємо значення змінної циклу для горутини
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			result := checker.Check(url)

			resultsMutex.Lock()
			results = append(results, result)
			stats.Add(result)
			resultsMutex.Unlock()
		}()
	}

	waitGroup.Wait()
	sortResults(results)
	return results, time.Since(start), stats.Snapshot()
}

// runConcurrentChannels передає результати через канал, а окрема
// горутина-координатор закриває його після завершення всіх перевірок.
func runConcurrentChannels(checker Checker, urls []string) ([]CheckResult, time.Duration) {
	start := time.Now()

	resultsChan := make(chan CheckResult, len(urls))
	var waitGroup sync.WaitGroup

	for _, url := range urls {
		url := url
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			resultsChan <- checker.Check(url)
		}()
	}

	go func() {
		waitGroup.Wait()
		close(resultsChan)
	}()

	results := make([]CheckResult, 0, len(urls))
	for result := range resultsChan {
		results = append(results, result)
	}

	sortResults(results)
	return results, time.Since(start)
}

func sortResults(results []CheckResult) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].URL < results[j].URL
	})
}

func main() {
	urls := []string{
		"https://go.dev",
		"https://github.com",
		"https://httpbin.org/status/404",
		"https://example.com",
		"https://invalid-fake-host-test.org",
	}

	checker := NewHTTPChecker(8 * time.Second)

	fmt.Println("=== HTTP URL Health Checker ===")

	fmt.Println("\n[Послідовна перевірка адрес...]")
	sequentialResults, sequentialDuration := runSequential(checker, urls)
	printResults(sequentialResults)
	printStats("послідовний режим", statsFromResults(sequentialResults), sequentialDuration)

	fmt.Println("\n[Конкурентна перевірка: горутини + Mutex...]")
	mutexResults, mutexDuration, mutexStats := runConcurrentMutex(checker, urls)
	printResults(mutexResults)
	printStats("горутини + Mutex", mutexStats, mutexDuration)

	fmt.Println("\n[Конкурентна перевірка: канали...]")
	channelResults, channelDuration := runConcurrentChannels(checker, urls)
	printResults(channelResults)
	printStats("канали", statsFromResults(channelResults), channelDuration)

	speedup := float64(sequentialDuration) / float64(channelDuration)
	fmt.Printf("\nПрискорення канального режиму: %.2fx\n", speedup)
}
