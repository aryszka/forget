package forget

import "math/rand"

const (
	testKeyCount  = 3000
	minKeyLength  = 6
	maxKeyLength  = 30
	minDataLength = 48
	maxDataLength = 900
)

var (
	testKeys []string
	testData [][]byte
)

func init() {
	testKeys = randomStrings(testKeyCount, minKeyLength, maxKeyLength)
	testData = randomByteSlices(testKeyCount, minDataLength, maxDataLength)
}

func randomBytes(min, max int) []byte {
	l := min + rand.Intn(max-min)
	p := make([]byte, l)
	rand.Read(p)
	return p
}

func randomByteSlices(n, min, max int) [][]byte {
	b := make([][]byte, n)
	for i := 0; i < n; i++ {
		b[i] = randomBytes(min, max)
	}

	return b
}

func randomString(min, max int) string {
	return string(randomBytes(min, max))
}

func randomStrings(n, min, max int) []string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = randomString(min, max)
	}

	return s
}

func randomKey() string {
	return testKeys[rand.Intn(len(testKeys))]
}

func randomData() []byte {
	return testData[rand.Intn(len(testData))]
}
