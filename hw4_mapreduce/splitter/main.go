package splitter

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

type SplitResponse struct {
	Input   string   `json:"input"`
	Parts   int      `json:"parts"`
	Chunks  []string `json:"chunks"`
	Bytes   int      `json:"bytes"`
	Lines   int      `json:"lines"`
	Message string   `json:"message,omitempty"`
}

func main() {
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	r.GET("/split", handleSplit)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	_ = r.Run("0.0.0.0:" + port)
}

func handleSplit(c *gin.Context) {
	inputS3 := c.Query("input_s3")
	if inputS3 == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing input_s3=s3://bucket/key"})
		return
	}

	parts, err := validateParts(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	bucket, key, err := parseS3URL(inputS3)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()
	s3c, err := newS3Client(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to init aws sdk: " + err.Error()})
		return
	}

	lines, nbytes, nlines, err := readS3ObjectAsLines(ctx, s3c, bucket, key)
	if err != nil {
		c.JSON(500, gin.H{"error": "read input failed: " + err.Error()})
		return
	}

	chunkLines := splitLinesEvenly(lines, parts)
	outPrefix := c.DefaultQuery("out_prefix", "chunks")

	chunkURLs, err := writeChunks(ctx, c, s3c, bucket, outPrefix, chunkLines)
	if err != nil {
		return
	}

	c.JSON(200, SplitResponse{
		Input:  inputS3,
		Parts:  parts,
		Chunks: chunkURLs,
		Bytes:  nbytes,
		Lines:  nlines,
	})
}

func validateParts(c *gin.Context) (int, error) {
	partsStr := c.DefaultQuery("parts", "3")
	parts, err := strconv.Atoi(partsStr)
	if err != nil || parts <= 0 || parts > 50 {
		return 0, fmt.Errorf("parts must be an int in [1..50]")
	}
	return parts, nil
}

func writeChunks(ctx context.Context, c *gin.Context, s3c *s3.Client, bucket, outPrefix string, chunkLines [][]string) ([]string, error) {
	chunkURLs := make([]string, 0, len(chunkLines))

	for i := 0; i < len(chunkLines); i++ {
		chunkKey := fmt.Sprintf("%s/chunk-%d.txt", strings.TrimSuffix(outPrefix, "/"), i)
		content := formatChunkContent(chunkLines[i])

		if err := putS3Object(ctx, s3c, bucket, chunkKey, content, "text/plain"); err != nil {
			c.JSON(500, gin.H{"error": "write chunk failed: " + err.Error()})
			return nil, err
		}
		chunkURLs = append(chunkURLs, fmt.Sprintf("s3://%s/%s", bucket, chunkKey))
	}

	return chunkURLs, nil
}

func formatChunkContent(lines []string) string {
	content := strings.Join(lines, "\n")
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return content
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

func readS3ObjectAsLines(ctx context.Context, s3c *s3.Client, bucket, key string) ([]string, int, int, error) {
	out, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, 0, 0, err
	}
	defer out.Body.Close()

	scanner := bufio.NewScanner(out.Body)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 4*1024*1024)

	var lines []string
	totalBytes := 0
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		totalBytes += len(line) + 1
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, err
	}

	return lines, totalBytes, len(lines), nil
}

func splitLinesEvenly(lines []string, parts int) [][]string {
	chunks := make([][]string, parts)
	for i := 0; i < parts; i++ {
		chunks[i] = []string{}
	}

	// Round-robin line distribution keeps chunks similar size
	for idx, line := range lines {
		chunks[idx%parts] = append(chunks[idx%parts], line)
	}
	return chunks
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
