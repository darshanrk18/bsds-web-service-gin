package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

type ReduceResponse struct {
	Inputs  []string    `json:"inputs"`
	Final   string      `json:"final"`
	Top10   [][2]any    `json:"top10"`
	Unique  int         `json:"uniqueWords"`
	Total   int         `json:"totalWords"`
	Millis  int64       `json:"ms"`
	Created string      `json:"createdAt"`
}

func main() {
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	r.GET("/reduce", handleReduce)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	_ = r.Run("0.0.0.0:" + port)
}

func handleReduce(c *gin.Context) {
	start := time.Now()
	inputsStr := c.Query("inputs")
	if inputsStr == "" {
		c.JSON(400, gin.H{"error": "missing inputs=s3://.../map0.json,s3://.../map1.json,... "})
		return
	}
	inputs := splitCSV(inputsStr)
	if len(inputs) < 1 {
		c.JSON(400, gin.H{"error": "inputs must contain at least 1 s3 url"})
		return
	}

	ctx := context.Background()
	s3c, err := newS3Client(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to init aws sdk: " + err.Error()})
		return
	}

	// All results assumed in same bucket; if not, you can enhance.
	bucket, _, err := parseS3URL(inputs[0])
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	merged, totalWords, err := mergeMapResults(ctx, c, s3c, bucket, inputs)
	if err != nil {
		return
	}

	outPrefix := c.DefaultQuery("out_prefix", "final")
	outKey := fmt.Sprintf("%s/final.json", strings.TrimSuffix(outPrefix, "/"))

	finalPayload := struct {
		Inputs  []string       `json:"inputs"`
		Counts  map[string]int `json:"counts"`
		Unique  int            `json:"uniqueWords"`
		Total   int            `json:"totalWords"`
		Created string         `json:"createdAt"`
	}{
		Inputs:  inputs,
		Counts:  merged,
		Unique:  len(merged),
		Total:   totalWords,
		Created: time.Now().Format(time.RFC3339),
	}

	b, _ := json.Marshal(finalPayload)
	if err := putS3Object(ctx, s3c, bucket, outKey, string(b), "application/json"); err != nil {
		c.JSON(500, gin.H{"error": "write final failed: " + err.Error()})
		return
	}

	outURL := fmt.Sprintf("s3://%s/%s", bucket, outKey)
	top10 := computeTopK(merged, 10)

	c.JSON(200, ReduceResponse{
		Inputs:  inputs,
		Final:   outURL,
		Top10:   top10,
		Unique:  len(merged),
		Total:   totalWords,
		Millis:  time.Since(start).Milliseconds(),
		Created: time.Now().Format(time.RFC3339),
	})
}

func mergeMapResults(ctx context.Context, c *gin.Context, s3c *s3.Client, bucket string, inputs []string) (map[string]int, int, error) {
	merged := map[string]int{}
	totalWords := 0

	for _, in := range inputs {
		b, key, err := parseS3URL(in)
		if err != nil {
			c.JSON(400, gin.H{"error": "bad s3 url: " + err.Error()})
			return nil, 0, err
		}
		if b != bucket {
			c.JSON(400, gin.H{"error": "all inputs must be in same bucket for this simple version"})
			return nil, 0, fmt.Errorf("bucket mismatch")
		}

		raw, err := readS3ObjectAsString(ctx, s3c, bucket, key)
		if err != nil {
			c.JSON(500, gin.H{"error": "read map json failed: " + err.Error()})
			return nil, 0, err
		}

		var payload struct {
			Counts map[string]int `json:"counts"`
			Total  int            `json:"totalWords"`
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			c.JSON(500, gin.H{"error": "invalid json from mapper: " + err.Error()})
			return nil, 0, err
		}

		for w, cnt := range payload.Counts {
			merged[w] += cnt
		}
		totalWords += payload.Total
	}

	return merged, totalWords, nil
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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

func putS3Object(ctx context.Context, s3c *s3.Client, bucket, key, content, contentType string) error {
	_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        strings.NewReader(content),
		ContentType: &contentType,
	})
	return err
}

func computeTopK(m map[string]int, k int) [][2]any {
	type kv struct {
		Key string
		Val int
	}
	arr := make([]kv, 0, len(m))
	for w, c := range m {
		arr = append(arr, kv{w, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].Val == arr[j].Val {
			return arr[i].Key < arr[j].Key
		}
		return arr[i].Val > arr[j].Val
	})
	if len(arr) > k {
		arr = arr[:k]
	}
	out := make([][2]any, 0, len(arr))
	for _, e := range arr {
		out = append(out, [2]any{e.Key, e.Val})
	}
	return out
}