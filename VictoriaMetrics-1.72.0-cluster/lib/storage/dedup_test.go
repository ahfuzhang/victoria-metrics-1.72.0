package storage

import (
	"reflect"
	"testing"
	"time"
)

func TestNeedsDedup(t *testing.T) {
	f := func(interval int64, timestamps []int64, expectedResult bool) {
		t.Helper()
		result := needsDedup(timestamps, interval)
		if result != expectedResult {
			t.Fatalf("unexpected result for needsDedup(%d, %d); got %v; want %v", timestamps, interval, result, expectedResult)
		}
	}
	f(-1, nil, false)
	f(-1, []int64{1}, false)
	f(0, []int64{1, 2}, false)
	f(10, []int64{1}, false)
	f(10, []int64{1, 2}, true)
	f(10, []int64{9, 10}, false)
	f(10, []int64{9, 10, 19}, true)
	f(10, []int64{9, 19}, false)
	f(10, []int64{0, 9, 19}, true)
	f(10, []int64{0, 19}, false)
	f(10, []int64{0, 35, 40}, false)
	f(10, []int64{0, 35, 40, 41}, true)
}

func TestDeduplicateSamples(t *testing.T) {
	// Disable deduplication before exit, since the rest of tests expect disabled dedup.

	f := func(scrapeInterval time.Duration, timestamps, timestampsExpected []int64) {
		t.Helper()
		timestampsCopy := make([]int64, len(timestamps))
		values := make([]float64, len(timestamps))
		for i, ts := range timestamps {
			timestampsCopy[i] = ts
			values[i] = float64(i)
		}
		dedupInterval := scrapeInterval.Milliseconds()
		timestampsCopy, values = DeduplicateSamples(timestampsCopy, values, dedupInterval)
		if !reflect.DeepEqual(timestampsCopy, timestampsExpected) {
			t.Fatalf("invalid DeduplicateSamples(%v) result;\ngot\n%v\nwant\n%v", timestamps, timestampsCopy, timestampsExpected)
		}
		// Verify values
		if len(timestampsCopy) == 0 {
			if len(values) != 0 {
				t.Fatalf("values must be empty; got %v", values)
			}
			return
		}
		j := 0
		for i, ts := range timestamps {
			if ts != timestampsCopy[j] {
				continue
			}
			if values[j] != float64(i) {
				t.Fatalf("unexpected value at index %d; got %v; want %v; values: %v", j, values[j], i, values)
			}
			j++
			if j == len(timestampsCopy) {
				break
			}
		}
		if j != len(timestampsCopy) {
			t.Fatalf("superfluous timestamps found starting from index %d: %v", j, timestampsCopy[j:])
		}

		// Verify that the second call to DeduplicateSamples doesn't modify samples.
		valuesCopy := append([]float64{}, values...)
		timestampsCopy, valuesCopy = DeduplicateSamples(timestampsCopy, valuesCopy, dedupInterval)
		if !reflect.DeepEqual(timestampsCopy, timestampsExpected) {
			t.Fatalf("invalid DeduplicateSamples(%v) timestamps for the second call;\ngot\n%v\nwant\n%v", timestamps, timestampsCopy, timestampsExpected)
		}
		if !reflect.DeepEqual(valuesCopy, values) {
			t.Fatalf("invalid DeduplicateSamples(%v) values for the second call;\ngot\n%v\nwant\n%v", timestamps, values, valuesCopy)
		}
	}
	f(time.Millisecond, nil, []int64{})
	f(time.Millisecond, []int64{123}, []int64{123})
	f(time.Millisecond, []int64{123, 456}, []int64{123, 456})
	f(time.Millisecond, []int64{0, 0, 0, 1, 1, 2, 3, 3, 3, 4}, []int64{0, 1, 2, 3, 4})
	f(0, []int64{0, 0, 0, 1, 1, 2, 3, 3, 3, 4}, []int64{0, 0, 0, 1, 1, 2, 3, 3, 3, 4})
	f(100*time.Millisecond, []int64{0, 100, 100, 101, 150, 180, 205, 300, 1000}, []int64{0, 100, 205, 300, 1000})
	f(10*time.Second, []int64{10e3, 13e3, 21e3, 22e3, 30e3, 33e3, 39e3, 45e3}, []int64{10e3, 21e3, 30e3, 45e3})
}

func TestDeduplicateSamplesDuringMerge(t *testing.T) {
	// Disable deduplication before exit, since the rest of tests expect disabled dedup.

	f := func(scrapeInterval time.Duration, timestamps, timestampsExpected []int64) {
		t.Helper()
		timestampsCopy := make([]int64, len(timestamps))
		values := make([]int64, len(timestamps))
		for i, ts := range timestamps {
			timestampsCopy[i] = ts
			values[i] = int64(i)
		}
		dedupInterval := scrapeInterval.Milliseconds()
		timestampsCopy, values = deduplicateSamplesDuringMerge(timestampsCopy, values, dedupInterval)
		if !reflect.DeepEqual(timestampsCopy, timestampsExpected) {
			t.Fatalf("invalid deduplicateSamplesDuringMerge(%v) result;\ngot\n%v\nwant\n%v", timestamps, timestampsCopy, timestampsExpected)
		}
		// Verify values
		if len(timestampsCopy) == 0 {
			if len(values) != 0 {
				t.Fatalf("values must be empty; got %v", values)
			}
			return
		}
		j := 0
		for i, ts := range timestamps {
			if ts != timestampsCopy[j] {
				continue
			}
			if values[j] != int64(i) {
				t.Fatalf("unexpected value at index %d; got %v; want %v; values: %v", j, values[j], i, values)
			}
			j++
			if j == len(timestampsCopy) {
				break
			}
		}
		if j != len(timestampsCopy) {
			t.Fatalf("superfluous timestamps found starting from index %d: %v", j, timestampsCopy[j:])
		}

		// Verify that the second call to DeduplicateSamples doesn't modify samples.
		valuesCopy := append([]int64{}, values...)
		timestampsCopy, valuesCopy = deduplicateSamplesDuringMerge(timestampsCopy, valuesCopy, dedupInterval)
		if !reflect.DeepEqual(timestampsCopy, timestampsExpected) {
			t.Fatalf("invalid deduplicateSamplesDuringMerge(%v) timestamps for the second call;\ngot\n%v\nwant\n%v", timestamps, timestampsCopy, timestampsExpected)
		}
		if !reflect.DeepEqual(valuesCopy, values) {
			t.Fatalf("invalid deduplicateSamplesDuringMerge(%v) values for the second call;\ngot\n%v\nwant\n%v", timestamps, values, valuesCopy)
		}
	}
	f(time.Millisecond, nil, []int64{})
	f(time.Millisecond, []int64{123}, []int64{123})
	f(time.Millisecond, []int64{123, 456}, []int64{123, 456})
	f(time.Millisecond, []int64{0, 0, 0, 1, 1, 2, 3, 3, 3, 4}, []int64{0, 1, 2, 3, 4})
	f(100*time.Millisecond, []int64{0, 100, 100, 101, 150, 180, 200, 300, 1000}, []int64{0, 100, 200, 300, 1000})
	f(10*time.Second, []int64{10e3, 13e3, 21e3, 22e3, 30e3, 33e3, 39e3, 45e3}, []int64{10e3, 21e3, 30e3, 45e3})

	var timestamps, timestampsExpected []int64
	for i := 0; i < 40; i++ {
		timestamps = append(timestamps, int64(i*1000))
		if i%2 == 0 {
			timestampsExpected = append(timestampsExpected, int64(i*1000))
		}
	}
	f(0, timestamps, timestamps)
	f(time.Second, timestamps, timestamps)
	f(2*time.Second, timestamps, timestampsExpected)
}
