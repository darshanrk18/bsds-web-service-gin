package mapper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

var nonLetters = regexp.MustCompile(`[^a-z]+`)

type MapResult struct {
	Chunk     string         `json:"chunk"`
	Output    string         `json:"output"`
	Counts    map[string]int `json:"counts,omitempty"`
	Unique    int            `json:"uniqueWords"`
	Total     int            `json:"totalWords"`
	Millis    int64          `json:"ms"`
	Message   string         `json:"message,omitempty"`
	OutPrefix string         `json:"out_prefix,omitempty"`
}

func main() {
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	r.GET("/map", func(c *gin.Context) {
		start := time.Now()
		chunkS3 := c.Query("chunk_s3")
		if chunkS3 == "" {
			c.JSON(400, gin.H{"error": "missing chunk_s3=s3://bucket/key"})
			return
		}
		bucket, key, err := parseS3URL(chunkS3)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		ctx := context.Background()
		s3c, err := newS3Client(ctx)
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to init aws sdk: " + err.Error()})
			return
		}

		text, err := readS3ObjectAsString(ctx, s3c, bucket, key)
		if err != nil {
			c.JSON(500, gin.H{"error": "read chunk failed: " + err.Error()})
			return
		}

		counts, total := countWords(text)
		unique := len(counts)

		// Determine output name; if chunk key contains chunk-#, use it.
		outPrefix := c.DefaultQuery("out_prefix", "maps")
		mapIndex := inferIndexFromKey(key)
		outKey := fmt.Sprintf("%s/map-%s.json", strings.TrimSuffix(outPrefix, "/"), mapIndex)

		payload := struct {
			Chunk   string         `json:"chunk"`
			Counts  map[string]int `json:"counts"`
			Unique  int            `json:"uniqueWords"`
			Total   int            `json:"totalWords"`
			Created string         `json:"createdAt"`
		}{
			Chunk:   chunkS3,
			Counts:  counts,
			Unique:  unique,
			Total:   total,
			Created: time.Now().Format(time.RFC3339),
		}

		b, _ := json.Marshal(payload)
		if err := putS3Object(ctx, s3c, bucket, outKey, string(b), "application/json"); err != nil {
			c.JSON(500, gin.H{"error": "write map output failed: " + err.Error()})
			return
		}

		outURL := fmt.Sprintf("s3://%s/%s", bucket, outKey)
		c.JSON(200, MapResult{
			Chunk:     chunkS3,
			Output:    outURL,
			Unique:    unique,
			Total:     total,
			Millis:    time.Since(start).Milliseconds(),
			OutPrefix: outPrefix,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	_ = r.Run("0.0.0.0:" + port)
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func parseS3URL(u string) (bucket, key string, err error) {
	if !strings.HasPrefix(u, "s3://") {
		return "", "", fmt.Errorf("invalid s3 url, expected s3://bucket/key")
	}
	trim := strings.TrimPrefix(u, "s3://")
	parts := strings.SplitN(trim, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 url, expected s3://bucket/key")
	}
	return parts[0], parts[1], nil
}

func readS3ObjectAsString(ctx context.Context, s3c *s3.Client, bucket, key string) (string, error) {
	out, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func countWords(s string) (map[string]int, int) {
	s = strings.ToLower(s)
	s = nonLetters.ReplaceAllString(s, " ")
	fields := strings.Fields(s)

	counts := make(map[string]int, len(fields))
	for _, w := range fields {
		counts[w]++
	}
	return counts, len(fields)
}

func inferIndexFromKey(key string) string {
	// expect ...chunk-0.txt => 0, else "x"
	base := key
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	// chunk-3.txt -> 3
	base = strings.TrimSuffix(base, ".txt")
	if strings.HasPrefix(base, "chunk-") {
		return strings.TrimPrefix(base, "chunk-")
	}
	return "x"
}

func putS3Object(ctx context.Context, s3c *s3.Client, bucket, key, content, contentType string) error {
	_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        strings.NewReader(content),
		ContentType: &contentType,
	})
	return err
}
