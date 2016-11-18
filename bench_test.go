package forget

import "testing"

func benchmarkGet(b *testing.B, n int) {
	c := New(Options{MaxSize: n * (maxKeyLength + maxDataLength)})
	for i := 0; i < n; i++ {
		c.Set(randomKeySpace(), randomKey(), randomData(), randomTTL())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(randomKeySpace(), randomKey())
	}
}

func benchmarkSet(b *testing.B, n int) {
	c := New(Options{MaxSize: 4 * n * (maxKeyLength + maxDataLength)})
	for i := 0; i < n; i++ {
		c.Set(randomKeySpace(), randomKey(), randomData(), randomTTL())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(randomKeySpace(), randomKey(), randomData(), randomTTL())
	}
}

func BenchmarkGet1(b *testing.B)       { benchmarkGet(b, 1) }
func BenchmarkGet10(b *testing.B)      { benchmarkGet(b, 10) }
func BenchmarkGet100(b *testing.B)     { benchmarkGet(b, 100) }
func BenchmarkGet1000(b *testing.B)    { benchmarkGet(b, 1000) }
func BenchmarkGet10000(b *testing.B)   { benchmarkGet(b, 10000) }
func BenchmarkGet100000(b *testing.B)  { benchmarkGet(b, 100000) }
func BenchmarkGet1000000(b *testing.B) { benchmarkGet(b, 1000000) }

func BenchmarkSet1(b *testing.B)       { benchmarkSet(b, 1) }
func BenchmarkSet10(b *testing.B)      { benchmarkSet(b, 10) }
func BenchmarkSet100(b *testing.B)     { benchmarkSet(b, 100) }
func BenchmarkSet1000(b *testing.B)    { benchmarkSet(b, 1000) }
func BenchmarkSet10000(b *testing.B)   { benchmarkSet(b, 10000) }
func BenchmarkSet100000(b *testing.B)  { benchmarkSet(b, 100000) }
func BenchmarkSet1000000(b *testing.B) { benchmarkSet(b, 1000000) }
