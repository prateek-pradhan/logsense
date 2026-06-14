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
