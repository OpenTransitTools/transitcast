// Package httpclient provides basic http functions
package httpclient

import (
	"io"
	"net/http"
	"os"
	"time"
)

// RemoteFileInfo contains information
type RemoteFileInfo struct {
	ETag                  string
	LastModifiedTimestamp int64
	Path                  string
}

// GetRemoteFileInfo retrieves ETag and last modified timestamp from url using a HEAD request
func GetRemoteFileInfo(url string) (RemoteFileInfo, error) {
	resp, err := http.Head(url)
	if err != nil {
		return RemoteFileInfo{}, err
	}
	return getRemoteFileInfo(url, resp), nil
}

func getRemoteFileInfo(url string, resp *http.Response) RemoteFileInfo {
	result := RemoteFileInfo{
		Path: url,
	}
	result.ETag = resp.Header.Get("ETag")

	lastModifiedString := resp.Header.Get("Last-Modified")

	if len(lastModifiedString) > 0 {
		parsedTime, err := time.Parse(time.RFC1123, lastModifiedString)
		if err == nil {
			result.LastModifiedTimestamp = parsedTime.Unix()
		}
	}
	return result

}

func (df *RemoteFileInfo) IsDifferent(etag string, lastModifiedTimestamp int64) bool {
	if len(df.ETag) > 0 {
		return df.ETag != etag
	}
	return df.LastModifiedTimestamp != lastModifiedTimestamp
}

// DownloadedFile contains information about a file that has been downloaded to the local file system
type DownloadedFile struct {
	RemoteFileInfo RemoteFileInfo
	LocalFilePath  string
	Size           int64
	DownloadedAt   time.Time
}

// DownloadRemoteFile retrieves a file from a url to a local file destination.
// On success returns information about the file in DownloadedFile
func DownloadRemoteFile(destinationFileName string, url string) (*DownloadedFile, error) {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	// Create the file
	out, err := os.Create(destinationFileName)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = out.Close()
	}()
	// Write the body to file
	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		return nil, err
	}
	remoteFileInfo := getRemoteFileInfo(url, resp)

	result := DownloadedFile{
		RemoteFileInfo: remoteFileInfo,
		LocalFilePath:  destinationFileName,
		Size:           bytesWritten,
		DownloadedAt:   time.Now(),
	}
	return &result, err
}
