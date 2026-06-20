package storage

import (
	"context"
	"github.com/prateek-pradhan/logsense/pkg/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Store struct {
	client *mongo.Client
	logs   *mongo.Collection
}

func Connect(ctx context.Context, uri string) (*Store, error) {
	opts := options.Client().ApplyURI(uri).SetWriteConcern(writeconcern.Majority())

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	logs := client.Database("logsense").Collection("logs")
	return &Store{client: client, logs: logs}, nil
}

func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *Store) BulkInsert(ctx context.Context, events []schema.LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(events))
	for i := range events {
		model := mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": events[i].ID}).SetReplacement(events[i]).SetUpsert(true)
		models = append(models, model)
	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := s.logs.BulkWrite(ctx, models, opts)
	return err
}

func (s *Store) ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	found := make(map[string]struct{}, len(ids))
	const chunk = 1000
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		filter := bson.M{"_id": bson.M{"$in": ids[start:end]}}
		opts := options.Find().SetProjection(bson.M{"_id": 1})
		cursor, err := s.logs.Find(ctx, filter, opts)
		if err != nil {
			return nil, err
		}

		for cursor.Next(ctx) {
			var doc struct {
				ID string `bson:"_id"`
			}
			if err := cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return nil, err
			}

			found[doc.ID] = struct{}{}
		}
		cursor.Close(ctx)
	}
	return found, nil
}

func (s *Store) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "service", Value: 1}, {Key: "event_time", Value: -1}}},
		{Keys: bson.D{{Key: "severity", Value: 1}, {Key: "event_time", Value: -1}}},
		{Keys: bson.D{{Key: "trace_id", Value: 1}}, Options: options.Index().SetSparse(true)},
		{Keys: bson.D{{Key: "ingested_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(604800)},
	}
	_, err := s.logs.Indexes().CreateMany(ctx, models)
	return err
}
