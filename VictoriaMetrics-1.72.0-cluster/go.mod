module github.com/VictoriaMetrics/VictoriaMetrics

go 1.17

require (
	cloud.google.com/go/storage v1.18.2
	github.com/VictoriaMetrics/fastcache v1.8.0

	// Do not use the original github.com/valyala/fasthttp because of issues
	// like https://github.com/valyala/fasthttp/commit/996610f021ff45fdc98c2ce7884d5fa4e7f9199b
	github.com/VictoriaMetrics/fasthttp v1.1.0
	github.com/VictoriaMetrics/metrics v1.18.1
	github.com/VictoriaMetrics/metricsql v0.37.0
	github.com/aws/aws-sdk-go v1.42.35
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/golang/snappy v0.0.4
	github.com/influxdata/influxdb v1.9.5
	github.com/klauspost/compress v1.14.1
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/prometheus/prometheus v1.8.2-0.20201119142752-3ad25a6dc3d9
	github.com/urfave/cli/v2 v2.3.0
	github.com/valyala/fastjson v1.6.3
	github.com/valyala/fastrand v1.1.0
	github.com/valyala/fasttemplate v1.2.1
	github.com/valyala/gozstd v1.15.1
	github.com/valyala/quicktemplate v1.7.0
	golang.org/x/net v0.0.0-20220114011407-0dd24b26b47d
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9
	google.golang.org/api v0.65.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go/compute v1.0.0 // indirect
	cloud.google.com/go/iam v0.1.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/go-kit/kit v0.12.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	google.golang.org/genproto v0.0.0-20220114231437-d2e6a121cae0 // indirect
	google.golang.org/grpc v1.43.0 // indirect
)
