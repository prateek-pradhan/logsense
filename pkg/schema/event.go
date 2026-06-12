package schema

import "time"

type LogEvent struct {
	ID         string            `json:"id" bson:"_id"`
	Service    string            `json:"service" bson:"service"`
	Severity   string            `json:"severity" bson:"severity"`
	Message    string            `json:"message" bson:"message"`
	EventTime  time.Time         `json:"event_time" bson:"event_time"`
	IngestedAt time.Time         `json:"ingested_at" bson:"ingested_at"`
	TraceID    string            `json:"trace_id" bson:"trace_id, omitempty"`
	Attrs      map[string]string `json:"attrs" bson:"attrs, omitempty"`
}
