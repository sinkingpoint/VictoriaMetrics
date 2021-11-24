package storage

import "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"

type ExemplarRow struct {
	MetricNameRaw []byte

	Labels    []Tag
	Timestamp int64
	Value     float64
}

func (e *ExemplarRow) InitFromPB(metricNameRaw []byte, rawExemplar *prompb.Exemplar) {
	for i := range e.Labels {
		e.Labels[i].Reset()
	}

	e.MetricNameRaw = metricNameRaw
	if n := len(rawExemplar.Labels) - cap(e.Labels); n > 0 {
		e.Labels = append(e.Labels[:cap(e.Labels)], make([]Tag, n)...)
	}
	e.Labels = e.Labels[:0]

	for i := range rawExemplar.Labels {
		lbl := &rawExemplar.Labels[i]
		e.Labels = append(e.Labels, Tag{
			Key:   lbl.Name,
			Value: lbl.Value,
		})
	}

	e.Timestamp = rawExemplar.Timestamp
	e.Value = rawExemplar.Value
}

type rawExemplar struct {
	TSID TSID

	Labels        []Tag
	Timestamp     int64
	Value         float64
	PrecisionBits uint8
}

type rawExemplarMarshaler struct{}

func (rem *rawExemplarMarshaler) marshalToInmemoryPart(mp *inmemoryPart, exemplars []rawExemplar) {

}
