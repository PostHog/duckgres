package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	originFetchesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_origin_fetches_total",
		Help: "Total origin fetch outcomes for cacheable misses",
	}, []string{"outcome"})
	originFetchRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_origin_fetch_retries_total",
		Help: "Total origin fetch retries by reason",
	}, []string{"reason"})
	originFetchInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_origin_fetches_in_flight",
		Help: "Current number of origin fetches filling the local cache",
	})
	originFetchQueued = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_origin_fetches_queued",
		Help: "Current number of cacheable origin fetch leaders waiting for an origin concurrency slot",
	})
	originFetchQueueWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_proxy_origin_fetch_queue_wait_seconds",
		Help:    "Seconds cacheable origin fetch leaders spend waiting for an origin concurrency slot after the limit is saturated",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})
)
