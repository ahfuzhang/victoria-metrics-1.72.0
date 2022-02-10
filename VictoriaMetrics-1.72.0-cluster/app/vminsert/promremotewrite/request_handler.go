package promremotewrite

import (
	"net/http"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="promremotewrite"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="promremotewrite"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(at *auth.Token, req *http.Request) error {  // remote write协议的处理
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	return writeconcurrencylimiter.Do(func() error {  // 并发数限制为CPU核数的4倍
		return parser.ParseStream(req.Body, func(tss []prompb.TimeSeries) error {  // 对 prompb的数据decode
			return insertRows(at, tss, extraLabels)    // 对解析好的time series数据执行插入操作
		})
	})
}

func insertRows(at *auth.Token, timeseries []prompb.TimeSeries, extraLabels []prompbmarshal.Label) error {  // 数据的插入逻辑
	ctx := netstorage.GetInsertCtx()  // 从内存池获取
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	rowsTotal := 0
	hasRelabeling := relabel.HasRelabeling()
	for i := range timeseries {  //遍历请求过来的每个time series
		ts := &timeseries[i]
		rowsTotal += len(ts.Samples)
		ctx.Labels = ctx.Labels[:0]
		srcLabels := ts.Labels
		for _, srcLabel := range srcLabels {
			ctx.AddLabelBytes(srcLabel.Name, srcLabel.Value)  // 追加到一个数组中
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		if hasRelabeling {
			ctx.ApplyRelabeling()
		}
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		ctx.SortLabelsIfNeeded()  // 最好是打开，否则vm-storage不会再对标签排序
		storageNodeIdx := ctx.GetStorageNodeIdx(at, ctx.Labels)
		ctx.MetricNameBuf = ctx.MetricNameBuf[:0]
		samples := ts.Samples
		for i := range samples {
			r := &samples[i]
			if len(ctx.MetricNameBuf) == 0 {
				ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], at.AccountID, at.ProjectID, ctx.Labels)
			}
			if err := ctx.WriteDataPointExt(at, storageNodeIdx, ctx.MetricNameBuf, r.Timestamp, r.Value); err != nil {
				return err
			}
		}
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.Get(at).Add(rowsTotal)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
