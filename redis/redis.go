// redis.go
// client wrapper and connection

package redis

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps the Redis client.
type Client struct {
	rdb *redis.Client
}

// NewClient creates a new Redis client connected to the given address.
func NewClient(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr, // e.g., "localhost:6379"
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	return &Client{rdb: rdb}
}

// Close closes the Redis client connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
