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
	forwardRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_forward_requests_total",
		Help: "Requests handled by the uncached forward-proxy path, by method",
	}, []string{"method"})
	inflightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_inflight_requests",
		Help: "Requests currently being handled by HandleProxy",
	})
)
