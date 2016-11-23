package forget

import (
	"sync"
	"testing"
)

const emulateMultiCoreMode = 4

func createCache(n int) []*Cache {
	c := make([]*Cache, emulateMultiCoreMode)
	for i := 0; i < len(c); i++ {
		c[i] = New()
		for j := 0; j < n; j++ {
			c[i].SetBytes(randomKey(), randomData())
		}
	}

	return c
}

func runN(execute func(*Cache)) func([]*Cache, int) {
	return func(c []*Cache, n int) {
		for i := 0; i < n; i++ {
			execute(c[i%len(c)])
		}
	}
}

func runConcurrent(c []*Cache, total, concurrent int, run func([]*Cache, int)) {
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
	defer func() {
		for _, ci := range c {
			ci.Close()
		}
	}()

	b.ResetTimer()
	runConcurrent(c, b.N, concurrent, runN(execute))
}

func executeGet(c *Cache) {
	c.GetBytes(randomKey())
}

func executeSet(c *Cache) {
	c.SetBytes(randomKey(), randomData())
}

func BenchmarkGet_0_100000(b *testing.B)      { benchmark(b, 0, 100000, executeGet) }
func BenchmarkGet_10_100000(b *testing.B)     { benchmark(b, 10, 100000, executeGet) }
func BenchmarkGet_1000_100000(b *testing.B)   { benchmark(b, 1000, 100000, executeGet) }
func BenchmarkGet_100000_100000(b *testing.B) { benchmark(b, 100000, 100000, executeGet) }

func BenchmarkSet_0_100000(b *testing.B)      { benchmark(b, 0, 100000, executeSet) }
func BenchmarkSet_10_100000(b *testing.B)     { benchmark(b, 10, 100000, executeSet) }
func BenchmarkSet_1000_100000(b *testing.B)   { benchmark(b, 1000, 100000, executeSet) }
func BenchmarkSet_100000_100000(b *testing.B) { benchmark(b, 100000, 100000, executeSet) }

func BenchmarkGet_0_1000(b *testing.B)      { benchmark(b, 0, 1000, executeGet) }
func BenchmarkGet_10_1000(b *testing.B)     { benchmark(b, 10, 1000, executeGet) }
func BenchmarkGet_1000_1000(b *testing.B)   { benchmark(b, 1000, 1000, executeGet) }
func BenchmarkGet_100000_1000(b *testing.B) { benchmark(b, 100000, 1000, executeGet) }

func BenchmarkSet_0_1000(b *testing.B)      { benchmark(b, 0, 1000, executeSet) }
func BenchmarkSet_10_1000(b *testing.B)     { benchmark(b, 10, 1000, executeSet) }
func BenchmarkSet_1000_1000(b *testing.B)   { benchmark(b, 1000, 1000, executeSet) }
func BenchmarkSet_100000_1000(b *testing.B) { benchmark(b, 100000, 1000, executeSet) }

func BenchmarkGet_0_10(b *testing.B)      { benchmark(b, 0, 10, executeGet) }
func BenchmarkGet_10_10(b *testing.B)     { benchmark(b, 10, 10, executeGet) }
func BenchmarkGet_1000_10(b *testing.B)   { benchmark(b, 1000, 10, executeGet) }
func BenchmarkGet_100000_10(b *testing.B) { benchmark(b, 100000, 10, executeGet) }

func BenchmarkSet_0_10(b *testing.B)      { benchmark(b, 0, 10, executeSet) }
func BenchmarkSet_10_10(b *testing.B)     { benchmark(b, 10, 10, executeSet) }
func BenchmarkSet_1000_10(b *testing.B)   { benchmark(b, 1000, 10, executeSet) }
func BenchmarkSet_100000_10(b *testing.B) { benchmark(b, 100000, 10, executeSet) }

func BenchmarkGet_0_1(b *testing.B)      { benchmark(b, 0, 1, executeGet) }
func BenchmarkGet_10_1(b *testing.B)     { benchmark(b, 10, 1, executeGet) }
func BenchmarkGet_1000_1(b *testing.B)   { benchmark(b, 1000, 1, executeGet) }
func BenchmarkGet_100000_1(b *testing.B) { benchmark(b, 100000, 1, executeGet) }

func BenchmarkSet_0_1(b *testing.B)      { benchmark(b, 0, 1, executeSet) }
func BenchmarkSet_10_1(b *testing.B)     { benchmark(b, 10, 1, executeSet) }
func BenchmarkSet_1000_1(b *testing.B)   { benchmark(b, 1000, 1, executeSet) }
func BenchmarkSet_100000_1(b *testing.B) { benchmark(b, 100000, 1, executeSet) }
