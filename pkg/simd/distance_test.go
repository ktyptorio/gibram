package simd

import (
	"math"
	"testing"
)

func TestVectorMathKnownResults(t *testing.T) {
	vector := []float32{3, 4, 0, 0, 0, 0, 0, 0}
	zero := make([]float32, len(vector))

	assertClose(t, "cosine self-similarity", CosineSimilarity(vector, vector), 1)
	assertClose(t, "L2 norm", L2Norm(vector), 5)
	assertClose(t, "Euclidean distance", EuclideanDistance(vector, zero), 5)
	assertClose(t, "dot product", DotProduct(vector, vector), 25)
}

func assertClose(t *testing.T, name string, got, want float32) {
	t.Helper()
	if math.Abs(float64(got-want)) > 1e-5 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}
