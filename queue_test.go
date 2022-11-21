package pq

// Check concurrency 1 - 1_000_000
// concurrency(1)->17 ns; concurrency(2,3)->30-35 ns; concurrency(>3) ~29-32 ns
// Channel 65 - 450 ns
