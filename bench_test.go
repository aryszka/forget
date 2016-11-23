package forget

import (
	"sync"
	"testing"
)

func createCache(n int) *Cache {
	c := New()
	for i := 0; i < n; i++ {
		c.Set(randomKey(), randomData())
	}

	return c
}

func runN(execute func(*Cache)) func(*Cache, int) {
	return func(c *Cache, n int) {
		for i := 0; i < n; i++ {
			execute(c)
		}
	}
}

func runConcurrent(c *Cache, total, concurrent int, run func(*Cache, int)) {
	n := total / concurrent
	if total%concurrent != 0 {
		n++
	}

	var wg sync.WaitGroup
	for total > 0 {
		if total-n < 0 {
			n = total
		}

		wg.Add(1)
		go func(n int) {
			run(c, n)
			wg.Done()
		}(n)
		total -= n
	}

	wg.Wait()
}

func benchmark(b *testing.B, init, concurrent int, execute func(*Cache)) {
	c := createCache(init)
	b.ResetTimer()
	runConcurrent(c, b.N, concurrent, runN(execute))
}

func executeGet(c *Cache) {
	c.Get(randomKey())
}

func executeSet(c *Cache) {
	c.Set(randomKey(), randomData())
}

func BenchmarkGet_0_1(b *testing.B)       { benchmark(b, 0, 1, executeGet) }
func BenchmarkGet_100_1(b *testing.B)     { benchmark(b, 100, 1, executeGet) }
func BenchmarkGet_10000_1(b *testing.B)   { benchmark(b, 10000, 1, executeGet) }
func BenchmarkGet_1000000_1(b *testing.B) { benchmark(b, 1000000, 1, executeGet) }

func BenchmarkSet_0_1(b *testing.B)       { benchmark(b, 0, 1, executeSet) }
func BenchmarkSet_100_1(b *testing.B)     { benchmark(b, 100, 1, executeSet) }
func BenchmarkSet_10000_1(b *testing.B)   { benchmark(b, 10000, 1, executeSet) }
func BenchmarkSet_1000000_1(b *testing.B) { benchmark(b, 1000000, 1, executeSet) }
