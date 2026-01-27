package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// album represents a music record album with its details.
type album struct {
	ID 		string `json:"id"`
	Title 	string `json:"title"`
	Artist 	string `json:"artist"`
	Price 	float64 `json:"price"`
}

// errorResponse represents an error response structure.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// validateAlbum validates the album data and returns an error if invalid.
func validateAlbum(a *album) error {
	if strings.TrimSpace(a.ID) == "" {
		return fmt.Errorf("album ID is required and cannot be empty")
	}
	if strings.TrimSpace(a.Title) == "" {
		return fmt.Errorf("album title is required and cannot be empty")
	}
	if strings.TrimSpace(a.Artist) == "" {
		return fmt.Errorf("album artist is required and cannot be empty")
	}
	if a.Price < 0 {
		return fmt.Errorf("album price cannot be negative")
	}
	if a.Price == 0 {
		return fmt.Errorf("album price must be greater than zero")
	}
	return nil
}

// albumExists checks if an album with the given ID already exists.
func albumExists(id string) bool {
	for _, a := range albums {
		if a.ID == id {
			return true
		}
	}
	return false
}

// getAlbums returns the list of albums as JSON.
func getAlbums(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, albums)
}

// getAlbumByID locates the album whose ID value matches the id
// parameter sent by the client, then returns that album as a response.
func getAlbumByID(c *gin.Context) {
	id := c.Param("id")

	// Validate that ID is not empty
	if strings.TrimSpace(id) == "" {
		c.IndentedJSON(http.StatusBadRequest, errorResponse{
			Error:   "invalid_request",
			Message: "album ID cannot be empty",
		})
		return
	}

	for _, a := range albums {
		if a.ID == id {
			c.IndentedJSON(http.StatusOK, a)
			return
		}
	}
	
	c.IndentedJSON(http.StatusNotFound, errorResponse{
		Error:   "not_found",
		Message: fmt.Sprintf("album with ID '%s' not found", id),
	})
}

// postAlbums adds an album from JSON received in the request body.
func postAlbums(c *gin.Context) {
	var newAlbum album

	// Bind JSON and handle binding errors
	if err := c.BindJSON(&newAlbum); err != nil {
		c.IndentedJSON(http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: fmt.Sprintf("failed to parse request body: %v", err),
		})
		return
	}

	// Validate album data
	if err := validateAlbum(&newAlbum); err != nil {
		c.IndentedJSON(http.StatusBadRequest, errorResponse{
			Error:   "validation_error",
			Message: err.Error(),
		})
		return
	}

	// Check for duplicate ID
	if albumExists(newAlbum.ID) {
		c.IndentedJSON(http.StatusConflict, errorResponse{
			Error:   "duplicate_id",
			Message: fmt.Sprintf("album with ID '%s' already exists", newAlbum.ID),
		})
		return
	}

	// Add the new album
	albums = append(albums, newAlbum)
	c.IndentedJSON(http.StatusCreated, newAlbum)
}

// albums slice to seed record album data.
var albums = []album{
	{ID: "1", Title: "Blue Train", Artist: "John Coltrane", Price: 56.99},
	{ID: "2", Title: "Jeru", Artist: "Gerry Mulligan", Price: 17.99},
	{ID: "3", Title: "Sarah Vaughan and Clifford Brown", Artist: "Sarah Vaughan", Price: 39.99},
}

func main() {
	router := gin.Default()
	router.GET("/albums", getAlbums)
	router.GET("/albums/:id", getAlbumByID)
	router.POST("/albums", postAlbums)
	router.Run(":8080")
}