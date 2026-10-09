package telemetry

import (
	"context"
	"net/http"
	"os"
	"sync/atomic"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
)

var tracing atomic.Bool

// Tracing reports whether SetupTracing enabled an exporter.
func Tracing() bool { return tracing.Load() }

// SetupTracing exports spans over OTLP/gRPC when OTEL_EXPORTER_OTLP_ENDPOINT
// (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set, and is a no-op otherwise.
// W3C trace context is propagated either way. The returned function
// flushes and stops the exporter.
func SetupTracing(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName("gate-"+service)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	tracing.Store(true)
	return tp.Shutdown, nil
}

// HTTPHandler traces incoming requests when tracing is on.
func HTTPHandler(h http.Handler, operation string) http.Handler {
	if !Tracing() {
		return h
	}
	return otelhttp.NewHandler(h, operation)
}

// HTTPTransport traces outgoing requests (and propagates trace context to
// upstreams) when tracing is on.
func HTTPTransport(rt http.RoundTripper) http.RoundTripper {
	if !Tracing() {
		return rt
	}
	return otelhttp.NewTransport(rt)
}

// GRPCServerOptions and GRPCDialOptions trace gRPC calls when tracing is on.
func GRPCServerOptions() []grpc.ServerOption {
	if !Tracing() {
		return nil
	}
	return []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
}

func GRPCDialOptions() []grpc.DialOption {
	if !Tracing() {
		return nil
	}
	return []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
}
