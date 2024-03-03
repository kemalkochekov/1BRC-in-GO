package read

import "testing"

func BenchmarkReadOneBillionRows(b *testing.B) {
	// This function will be called multiple times by the testing framework.
	// You can customize the number of iterations by adjusting the 'b.N' value.
	for i := 0; i < b.N; i++ {

		err := Read("../../internal/data")
		if err != nil {
			b.Fatalf("Error reading data: %v", err)
		}
	}
}
