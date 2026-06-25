package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var queryLogKafkaWriterEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_query_log_kafka_writer_events_total",
	Help: "Query-log Kafka writer events by outcome.",
}, []string{"outcome"})
