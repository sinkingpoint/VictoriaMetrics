package storage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

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

type exemplarTags []Tag

func (e exemplarTags) marshal(dst []byte) []byte {
	// Tags are marshalled to [num tags, length, tags]
	dst = encoding.MarshalUint64(dst, uint64(len(e)))
	offset := len(dst)

	// Skip 8 bytes for the offset, we'll return to it later
	tagData := dst[8:]
	for i := range e {
		tag := e[i]
		tagData = tag.Marshal(tagData)
	}

	length := uint64(offset - len(tagData))
	encoding.MarshalUint64(dst, length)

	return tagData
}

type rawExemplar struct {
	TSID TSID

	Labels        []Tag
	Timestamp     int64
	Value         float64
	PrecisionBits uint8
}

type rawExemplarIndexBlock struct {
	TSID                  TSID
	timestampsOffset      uint64
	valuesOffset          uint64
	tagsOffset            uint64
	firstValue            int64
	minTimestamp          int64
	scale                 int16
	precisionBits         uint8
	valuesMarshalType     encoding.MarshalType
	timestampsMarshalType encoding.MarshalType
}

type rawExemplarMarshaler struct {
	currentOffset uint64

	rawValuesBuffer []float64
	valuesBuffer    []int64
	timestampBuffer []int64
	tagsBuffer      []exemplarTags

	data      []byte
	indexData []byte
}

func (rem *rawExemplarMarshaler) marshalToInmemoryPart(mp *inmemoryPart, exemplars []rawExemplar) {
	if len(exemplars) == 0 {
		return
	}

	// TODO: Sort the exemplars
	tsid := exemplars[0].TSID
	header := rawExemplarIndexBlock{}
	for i := range exemplars {
		ex := exemplars[i]
		if ex.TSID.MetricID == tsid.MetricID && len(rem.timestampBuffer) < maxRowsPerBlock {
			rem.timestampBuffer = append(rem.timestampBuffer, ex.Timestamp)
			rem.rawValuesBuffer = append(rem.rawValuesBuffer, ex.Value)
			rem.tagsBuffer = append(rem.tagsBuffer, ex.Labels)
			continue
		}

		valuesBuffer := rem.valuesBuffer

		valuesBuffer, header.scale = decimal.AppendFloatToDecimal(valuesBuffer, rem.rawValuesBuffer)
		header.precisionBits = ex.PrecisionBits
		header.timestampsOffset = rem.currentOffset
		start := len(rem.data)
		rem.data, header.valuesMarshalType, header.firstValue = encoding.MarshalValues(rem.data, valuesBuffer, ex.PrecisionBits)
		header.valuesOffset = rem.currentOffset + uint64(start-len(rem.data))
		start = len(rem.data)
		rem.data, header.timestampsMarshalType, header.minTimestamp = encoding.MarshalTimestamps(rem.data, rem.timestampBuffer, ex.PrecisionBits)
		header.tagsOffset = header.valuesOffset + uint64(start-len(rem.data))
		for j := range rem.tagsBuffer {
			tags := rem.tagsBuffer[j]
			rem.data = tags.marshal(rem.data)
		}

		fs.MustWriteData(&mp.exemplarData, rem.data)
		fs.MustWriteData(&mp.exemplarIndexData, rem.indexData)

		rem.data = rem.data[:0]
		rem.indexData = rem.indexData[:0]
	}
}
