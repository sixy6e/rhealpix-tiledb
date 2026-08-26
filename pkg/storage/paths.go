package storage

import (
	"net/url"
	"path"
	"strings"
)

// JoinURI correctly joins path components while preserving scheme prefixes like s3://
func JoinURI(base string, elem ...string) string {
	if !strings.Contains(base, "://") {
		// pure local path (e.g., "/home/user/catalog" or "data/catalog")
		all := append([]string{base}, elem...)
		return path.Join(all...)
	}

	u, err := url.Parse(base)
	if err != nil {
		// fallback if parsing fails
		return strings.TrimSuffix(base, "/") + "/" + path.Join(elem...)
	}

	// join the URL path portion and preserve scheme + host
	allPath := append([]string{u.Path}, elem...)
	u.Path = path.Join(allPath...)
	return u.String()
}
