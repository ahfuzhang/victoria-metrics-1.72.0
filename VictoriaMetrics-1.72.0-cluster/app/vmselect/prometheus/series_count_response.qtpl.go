// Code generated by qtc from "series_count_response.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

// SeriesCountResponse generates response for /api/v1/series/count .

//line app/vmselect/prometheus/series_count_response.qtpl:3
package prometheus

//line app/vmselect/prometheus/series_count_response.qtpl:3
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/series_count_response.qtpl:3
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmselect/prometheus/series_count_response.qtpl:3
func StreamSeriesCountResponse(qw422016 *qt422016.Writer, isPartial bool, n uint64) {
//line app/vmselect/prometheus/series_count_response.qtpl:3
	qw422016.N().S(`{"status":"success","isPartial":`)
//line app/vmselect/prometheus/series_count_response.qtpl:6
	if isPartial {
//line app/vmselect/prometheus/series_count_response.qtpl:6
		qw422016.N().S(`true`)
//line app/vmselect/prometheus/series_count_response.qtpl:6
	} else {
//line app/vmselect/prometheus/series_count_response.qtpl:6
		qw422016.N().S(`false`)
//line app/vmselect/prometheus/series_count_response.qtpl:6
	}
//line app/vmselect/prometheus/series_count_response.qtpl:6
	qw422016.N().S(`,"data":[`)
//line app/vmselect/prometheus/series_count_response.qtpl:7
	qw422016.N().DL(int64(n))
//line app/vmselect/prometheus/series_count_response.qtpl:7
	qw422016.N().S(`]}`)
//line app/vmselect/prometheus/series_count_response.qtpl:9
}

//line app/vmselect/prometheus/series_count_response.qtpl:9
func WriteSeriesCountResponse(qq422016 qtio422016.Writer, isPartial bool, n uint64) {
//line app/vmselect/prometheus/series_count_response.qtpl:9
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/series_count_response.qtpl:9
	StreamSeriesCountResponse(qw422016, isPartial, n)
//line app/vmselect/prometheus/series_count_response.qtpl:9
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/series_count_response.qtpl:9
}

//line app/vmselect/prometheus/series_count_response.qtpl:9
func SeriesCountResponse(isPartial bool, n uint64) string {
//line app/vmselect/prometheus/series_count_response.qtpl:9
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/series_count_response.qtpl:9
	WriteSeriesCountResponse(qb422016, isPartial, n)
//line app/vmselect/prometheus/series_count_response.qtpl:9
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/series_count_response.qtpl:9
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/series_count_response.qtpl:9
	return qs422016
//line app/vmselect/prometheus/series_count_response.qtpl:9
}
