//go:build embed || unit

package web

import (
	"net/http"
	"path"
	"strings"
)

// Vite emits content-hashed filenames under assets/, so the backend can apply
// immutable caching without relying on a reverse proxy to classify paths.
const staticAssetsCacheControl = "public, max-age=31536000, immutable"

// isFingerprintedEmbeddedAssetPath reports whether a cleaned URL path refers to
// a Vite asset whose filename contains the default eight-character build hash.
func isFingerprintedEmbeddedAssetPath(cleanPath string) bool {
	_, _, _, ok := embeddedAssetFingerprint(cleanPath)
	return ok
}

// embeddedAssetFingerprint extracts the directory, extension, and Vite
// fingerprint from a hashed asset path. The fingerprint is content-derived,
// so it can remain stable when a shared CSS chunk gets a different component
// name after a frontend refactor.
func embeddedAssetFingerprint(cleanPath string) (dir, extension, fingerprint string, ok bool) {
	cleanPath = strings.TrimPrefix(cleanPath, "/")
	if !strings.HasPrefix(cleanPath, "assets/") {
		return "", "", "", false
	}

	filename := path.Base(cleanPath)
	extension = path.Ext(filename)
	stem := strings.TrimSuffix(filename, extension)
	const fingerprintLength = 8
	delimiterIndex := len(stem) - fingerprintLength - 1
	if extension == "" || delimiterIndex < 1 || stem[delimiterIndex] != '-' {
		return "", "", "", false
	}

	// Vite hashes use URL-safe characters and are stable for immutable caching.
	fingerprint = stem[delimiterIndex+1:]
	for _, char := range fingerprint {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' || char == '-' {
			continue
		}
		return "", "", "", false
	}

	return path.Dir(cleanPath), extension, fingerprint, true
}

// applyStaticAssetCacheHeaders sets Cache-Control for long-cacheable static paths.
// index.html / SPA routes must keep no-cache and are not handled here.
func applyStaticAssetCacheHeaders(header http.Header, cleanPath string) {
	if header == nil || !isFingerprintedEmbeddedAssetPath(cleanPath) {
		return
	}
	header.Set("Cache-Control", staticAssetsCacheControl)
}
