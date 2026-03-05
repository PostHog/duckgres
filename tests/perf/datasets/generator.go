package datasets

import (
	"fmt"
	"math/rand"
)

type Record struct {
	CustomerID  int
	Region      string
	AmountCents int
}

func GenerateDeterministic(seed int64, scale int) ([]Record, error) {
	if scale <= 0 {
		return nil, fmt.Errorf("scale must be > 0")
	}
	rng := rand.New(rand.NewSource(seed))
	n := 100 * scale
	out := make([]Record, 0, n)
	regions := []string{"na", "eu", "apac"}
	for i := 0; i < n; i++ {
		out = append(out, Record{
			CustomerID:  i + 1,
			Region:      regions[rng.Intn(len(regions))],
			AmountCents: 100 + rng.Intn(100000),
		})
	}
	return out, nil
}
