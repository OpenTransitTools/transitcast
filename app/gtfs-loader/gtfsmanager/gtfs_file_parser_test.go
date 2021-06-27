package gtfsmanager

import (
	"strings"
	"testing"
	"time"
)

func TestGTFSFileParser_getString(t *testing.T) {
	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         string
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         "",
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         "",
			expectError:  false,
		},
		{
			name:         "first",
			askForColumn: "one",
			optional:     false,
			line:         "first,second",
			want:         "first",
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         "",
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         "",
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getString(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				if C.getError() != nil {
					t.Errorf("Received error after asking for %v ", tt.askForColumn)
				}
			}
			if got != tt.want {
				t.Errorf("getString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVFileParser_getInt(t *testing.T) {
	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         int
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         0,
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         0,
			expectError:  false,
		},
		{
			name:         "first",
			askForColumn: "one",
			optional:     false,
			line:         "277,second",
			want:         277,
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         0,
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         0,
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getInt(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				if C.getError() != nil {
					t.Errorf("Received error after asking for %v ", tt.askForColumn)
				}
			}
			if got != tt.want {
				t.Errorf("getInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVFileParser_getIntPointer(t *testing.T) {
	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         int
		expectNil    bool
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
		{
			name:         "first",
			askForColumn: "one",
			optional:     false,
			line:         "277,second",
			want:         277,
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getIntPointer(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				if C.getError() != nil {
					t.Errorf("Received error after asking for %v ", tt.askForColumn)
				}
			}
			if tt.expectNil {
				if got != nil {
					t.Errorf("Expected nil value")
				}
			} else if *got != tt.want {
				t.Errorf("getIntPointer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVFileParser_getFloat64Pointer(t *testing.T) {
	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         float64
		expectNil    bool
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
		{
			name:         "first",
			askForColumn: "one",
			optional:     false,
			line:         "277.32331,second",
			want:         277.32331,
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty whitespace",
			askForColumn: "one",
			optional:     false,
			line:         "  ,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getFloat64Pointer(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				if C.getError() != nil {
					t.Errorf("Received error after asking for %v ", tt.askForColumn)
				}
			}
			if tt.expectNil {
				if got != nil {
					t.Errorf("Expected nil value")
				}
			} else if *got != tt.want {
				t.Errorf("getFloat64Pointer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getTestDate(str string) time.Time {
	result, _ := time.Parse("20060102", str)
	return result
}

func TestCSVFileParser_getGTFSDatePointer(t *testing.T) {

	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         time.Time
		expectNil    bool
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         time.Time{},
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         time.Time{},
			expectNil:    true,
			expectError:  false,
		},
		{
			name:         "first",
			askForColumn: "one",
			optional:     false,
			line:         "20210627,second",
			want:         getTestDate("20210627"),
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         time.Time{},
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty whitespace",
			askForColumn: "one",
			optional:     false,
			line:         "  ,second",
			want:         time.Time{},
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         time.Time{},
			expectNil:    true,
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getGTFSDatePointer(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				if C.getError() != nil {
					t.Errorf("Received error after asking for %v ", tt.askForColumn)
				}
			}
			if tt.expectNil {
				if got != nil {
					t.Errorf("Expected nil value")
				}
			} else if *got != tt.want {
				t.Errorf("getGTFSDatePointer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVFileParser_getGTFSTimePointer(t *testing.T) {

	headers := "one,two"
	tests := []struct {
		name         string
		askForColumn string
		optional     bool
		line         string
		want         int
		expectNil    bool
		expectError  bool
	}{
		{
			name:         "missing",
			askForColumn: "three",
			optional:     false,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "missing optional",
			askForColumn: "three",
			optional:     true,
			line:         "first,second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
		{
			name:         "12 hours",
			askForColumn: "one",
			optional:     false,
			line:         "12:00:00,second",
			want:         12 * 60 * 60,
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "empty",
			askForColumn: "one",
			optional:     false,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty whitespace",
			askForColumn: "one",
			optional:     false,
			line:         "  ,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "empty optional",
			askForColumn: "one",
			optional:     true,
			line:         ",second",
			want:         0,
			expectNil:    true,
			expectError:  false,
		},
		{
			name:         "empty not optional",
			askForColumn: "one",
			optional:     false,
			line:         " ,second",
			want:         0,
			expectNil:    true,
			expectError:  true,
		},
		{
			name:         "8 hours (one digit in first location)",
			askForColumn: "one",
			optional:     false,
			line:         "8:00:00,second",
			want:         8 * 60 * 60,
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "13 hours, 26 minutes, 56 seconds",
			askForColumn: "one",
			optional:     false,
			line:         "13:26:56,second",
			want:         (13 * 60 * 60) + (26 * 60) + 56,
			expectNil:    false,
			expectError:  false,
		},
		{
			name:         "26 hours, 5 minutes, 1 seconds",
			askForColumn: "one",
			optional:     false,
			line:         "26:05:01,second",
			want:         (26 * 60 * 60) + (5 * 60) + 1,
			expectNil:    false,
			expectError:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileContents := headers + "\n" + tt.line
			C, _ := makeGTFSFileParser(strings.NewReader(fileContents), tt.name)
			_ = C.nextLine()
			got := C.getGTFSTimePointer(tt.askForColumn, tt.optional)
			if tt.expectError {
				if C.getError() == nil {
					t.Errorf("Expected error after asking for %v ", tt.askForColumn)
				}
			} else {
				err := C.getError()
				if err != nil {
					t.Errorf("Received error after asking for column %v. error:%v ", tt.askForColumn, err)
				}
			}
			if tt.expectNil {
				if got != nil {
					t.Errorf("Expected nil value")
				}
			} else if got == nil {
				t.Errorf("getGTFSTimePointer() = nil, want %v", tt.want)
			} else if *got != tt.want {
				t.Errorf("getGTFSTimePointer() = %v, want %v", *got, tt.want)
			}
		})
	}
}
