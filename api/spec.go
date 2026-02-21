package main

import "github.com/go-openapi/spec"

func buildOpenAPISpec() ([]byte, error) {
	sw := spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Swagger:  "2.0",
			BasePath: "/",
			Info: &spec.Info{InfoProps: spec.InfoProps{
				Title:       "CMMA API",
				Description: "BybitのOHLCVデータから価格変動率と出来高ランキングを返すAPI",
				Version:     "2.0.0-go",
			}},
			Consumes: []string{"application/json"},
			Produces: []string{"application/json"},
			Paths: &spec.Paths{Paths: map[string]spec.PathItem{
				"/":           {PathItemProps: spec.PathItemProps{Get: spec.NewOperation("root").WithSummary("Root endpoint").WithDescription("Service root endpoint").RespondsWith(200, schemaResponse("Root response", "#/definitions/RootResponse"))}},
				"/volatility": {PathItemProps: spec.PathItemProps{Get: volatilityOperation()}},
				"/volume":     {PathItemProps: spec.PathItemProps{Get: volumeOperation()}},
			}},
			Definitions: apiDefinitions(),
		},
	}

	return sw.MarshalJSON()
}

func volatilityOperation() *spec.Operation {
	tfParam := spec.QueryParam("timeframe").Typed("string", "").WithDescription("分析したいタイムフレーム。")
	tfParam.Required = true
	tfParam.Enum = toAnySlice(validTimeframes)

	thresholdParam := spec.QueryParam("threshold").Typed("number", "double").WithDescription("価格変動率の閾値(%)。絶対値で比較されます。例: 5.0")
	thresholdParam.Required = true
	thresholdParam.Minimum = float64Ptr(0)
	thresholdParam.ExclusiveMinimum = true

	offsetParam := spec.QueryParam("offset").Typed("integer", "int32").WithDescription("何本前のローソク足と比較するか。デフォルトは1。")
	offsetParam.Default = 1
	offsetParam.Minimum = float64Ptr(1)

	directionParam := spec.QueryParam("direction").Typed("string", "").WithDescription("変動方向をフィルタします。")
	directionParam.Default = "both"
	directionParam.Enum = []any{"up", "down", "both"}

	sortParam := spec.QueryParam("sort").Typed("string", "").WithDescription("結果のソート順。")
	sortParam.Default = "volatility_desc"
	sortParam.Enum = []any{"volatility_desc", "volatility_asc", "symbol_asc"}

	limitParam := spec.QueryParam("limit").Typed("integer", "int32").WithDescription("取得する最大件数。")
	limitParam.Default = 100
	limitParam.Minimum = float64Ptr(1)
	limitParam.Maximum = float64Ptr(500)

	op := spec.NewOperation("getVolatility").
		WithSummary("価格変動率の高い銘柄を取得").
		WithDescription("指定閾値を超える銘柄の変動率データを返します。").
		WithTags("volatility")
	op.Parameters = []spec.Parameter{*tfParam, *thresholdParam, *offsetParam, *directionParam, *sortParam, *limitParam}
	op.Responses = &spec.Responses{ResponsesProps: spec.ResponsesProps{StatusCodeResponses: map[int]spec.Response{
		200: *schemaResponse("成功", "#/definitions/VolatilityResponse"),
		400: *schemaResponse("不正なtimeframe", "#/definitions/ErrorResponse"),
		422: *schemaResponse("入力検証エラー", "#/definitions/ErrorResponse"),
		500: *schemaResponse("サーバーエラー", "#/definitions/ErrorResponse"),
	}}}
	return op
}

func volumeOperation() *spec.Operation {
	tfParam := spec.QueryParam("timeframe").Typed("string", "").WithDescription("出来高集計に使うOHLCVのタイムフレーム。")
	tfParam.Required = true
	tfParam.Enum = toAnySlice(validTimeframes)

	periodParam := spec.QueryParam("period").Typed("string", "").WithDescription("出来高を集計する期間 (例: 24h, 7d)。")
	periodParam.Required = true
	periodParam.Enum = toAnySlice(validPeriods)

	minVolumeParam := spec.QueryParam("min_volume").Typed("number", "double").WithDescription("期間内の合計出来高/売買代金での足切り値。")
	minVolumeParam.Minimum = float64Ptr(0)
	minVolumeParam.ExclusiveMinimum = true

	minVolumeTargetParam := spec.QueryParam("min_volume_target").Typed("string", "").WithDescription("min_volume の対象 (volume or turnover)。")
	minVolumeTargetParam.Default = "turnover"
	minVolumeTargetParam.Enum = []any{"volume", "turnover"}

	sortParam := spec.QueryParam("sort").Typed("string", "").WithDescription("結果のソート順。")
	sortParam.Default = "volume_desc"
	sortParam.Enum = []any{"volume_desc", "volume_asc", "turnover_desc", "turnover_asc", "symbol_asc"}

	limitParam := spec.QueryParam("limit").Typed("integer", "int32").WithDescription("取得する最大件数。")
	limitParam.Default = 100
	limitParam.Minimum = float64Ptr(1)
	limitParam.Maximum = float64Ptr(500)

	op := spec.NewOperation("getVolume").
		WithSummary("指定期間の出来高ランキングを取得").
		WithDescription("指定期間内の合計出来高・合計売買代金ランキングを返します。").
		WithTags("volume")
	op.Parameters = []spec.Parameter{*tfParam, *periodParam, *minVolumeParam, *minVolumeTargetParam, *sortParam, *limitParam}
	op.Responses = &spec.Responses{ResponsesProps: spec.ResponsesProps{StatusCodeResponses: map[int]spec.Response{
		200: *schemaResponse("成功", "#/definitions/VolumeResponse"),
		400: *schemaResponse("不正なtimeframe/period", "#/definitions/ErrorResponse"),
		422: *schemaResponse("入力検証エラー", "#/definitions/ErrorResponse"),
		500: *schemaResponse("サーバーエラー", "#/definitions/ErrorResponse"),
	}}}
	return op
}

func apiDefinitions() spec.Definitions {
	return spec.Definitions{
		"RootResponse": objectSchema(map[string]spec.Schema{"message": schemaWithDescription(*spec.StringProperty(), "ルートメッセージ")}, "message"),
		"ErrorDetail": objectSchema(map[string]spec.Schema{
			"code":    schemaWithDescription(*spec.StringProperty(), "エラーコード"),
			"message": schemaWithDescription(*spec.StringProperty(), "エラーメッセージ"),
		}, "code", "message"),
		"ErrorResponse": objectSchema(map[string]spec.Schema{"error": schemaWithDescription(*spec.RefSchema("#/definitions/ErrorDetail"), "エラー詳細")}, "error"),
		"PriceInfo": objectSchema(map[string]spec.Schema{
			"close":      schemaWithDescription(*spec.Float64Property(), "現在の足の終値"),
			"prev_close": schemaWithDescription(*spec.Float64Property(), "比較対象足の終値"),
		}, "close", "prev_close"),
		"ChangeInfo": objectSchema(map[string]spec.Schema{
			"pct":       schemaWithDescription(*spec.Float64Property(), "価格変動率 (%)"),
			"direction": schemaWithDescription(*spec.StringProperty(), "変動方向"),
		}, "pct", "direction"),
		"VolatilityData": objectSchema(map[string]spec.Schema{
			"symbol":    schemaWithDescription(*spec.StringProperty(), "銘柄シンボル"),
			"timeframe": schemaWithDescription(*spec.StringProperty(), "タイムフレーム"),
			"candle_ts": schemaWithDescription(*spec.Int64Property(), "ローソク足の開始タイムスタンプ (ミリ秒)"),
			"price":     schemaWithDescription(*spec.RefSchema("#/definitions/PriceInfo"), "価格情報"),
			"change":    schemaWithDescription(*spec.RefSchema("#/definitions/ChangeInfo"), "変動情報"),
		}, "symbol", "timeframe", "candle_ts", "price", "change"),
		"VolatilityResponse": objectSchema(map[string]spec.Schema{
			"count": schemaWithDescription(*spec.Int64Property(), "返却件数"),
			"data":  schemaWithDescription(*spec.ArrayProperty(spec.RefSchema("#/definitions/VolatilityData")), "変動率データ"),
		}, "count", "data"),
		"VolumeData": objectSchema(map[string]spec.Schema{
			"symbol":         schemaWithDescription(*spec.StringProperty(), "銘柄シンボル"),
			"total_volume":   schemaWithDescription(*spec.Float64Property(), "合計出来高"),
			"total_turnover": schemaWithDescription(*spec.Float64Property(), "合計売買代金"),
			"timeframe":      schemaWithDescription(*spec.StringProperty(), "タイムフレーム"),
			"period":         schemaWithDescription(*spec.StringProperty(), "集計期間"),
		}, "symbol", "total_volume", "total_turnover", "timeframe", "period"),
		"VolumeResponse": objectSchema(map[string]spec.Schema{
			"count": schemaWithDescription(*spec.Int64Property(), "返却件数"),
			"data":  schemaWithDescription(*spec.ArrayProperty(spec.RefSchema("#/definitions/VolumeData")), "出来高データ"),
		}, "count", "data"),
	}
}

func objectSchema(props map[string]spec.Schema, required ...string) spec.Schema {
	return spec.Schema{SchemaProps: spec.SchemaProps{Type: []string{"object"}, Properties: props, Required: required}}
}

func schemaWithDescription(s spec.Schema, desc string) spec.Schema {
	s.Description = desc
	return s
}

func schemaResponse(description, schemaRef string) *spec.Response {
	return spec.NewResponse().WithDescription(description).WithSchema(spec.RefSchema(schemaRef))
}

func toAnySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}

func float64Ptr(v float64) *float64 {
	return &v
}
