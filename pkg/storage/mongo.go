package storage

import (
	"context"
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
