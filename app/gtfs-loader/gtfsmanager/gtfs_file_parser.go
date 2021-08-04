package gtfsmanager

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

// gtfsRowReader interface defines methods used to read rows from a gtfs csv file and record them to a database
type gtfsRowReader interface {

	// addRow should read the current line from gtfsFileParser and records the resulting record with gtfsDataSetTx
	// or stores the record to be recorded in a batch later via flush
	addRow(parser *gtfsFileParser, gtfsDataSetTx *gtfs.DataSetTransaction) error

	// flush should record any pending records with gtfsDataSetTx, if any
	flush(dsTx *gtfs.DataSetTransaction) error
}

// gtfsFileParser holds information about a cvs file. Methods to read columns for records. Errors while extracting data types
// are stored in errors array which record the line number the error happened.
type gtfsFileParser struct {
	Filename       string
	line           int
	cvsReader      *csv.Reader
	headers        []string
	currentRecords []string
	errors         []error
}

// makeGTFSFileParser creates new gtfsFileParser from io.Reader
func makeGTFSFileParser(r io.Reader, filename string) (*gtfsFileParser, error) {
	csvReader := csv.NewReader(r)

	headers, err := csvReader.Read()

	if err != nil {
		return nil, fmt.Errorf("unable to load header in stop_times.txt file: %v", err)
	}
	return &gtfsFileParser{
		Filename:       filename,
		line:           1,
		cvsReader:      csvReader,
		headers:        headers,
		currentRecords: headers,
	}, nil
}

// getString retrieves string
// returns empty string if missing
func (C *gtfsFileParser) getString(name string, optional bool) string {
	result := C.getStringPointer(name, optional)
	if result == nil {
		return ""
	}
	return *result
}

// getStringPointer retrieves string pointer
// returns nil if missing
func (C *gtfsFileParser) getStringPointer(name string, optional bool) *string {
	result, err := findValue(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
	}
	return result
}

// getFloat64 retrieves float64
// returns 0 if missing.
func (C *gtfsFileParser) getFloat64(name string, optional bool) float64 {
	result := C.getFloat64Pointer(name, optional)
	if result == nil {
		return 0
	}
	return *result
}

// getFloat64Pointer retrieves float64 pointer
// returns nil if missing.
func (C *gtfsFileParser) getFloat64Pointer(name string, optional bool) *float64 {
	result, err := getFloat64(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
	}
	return result
}

// getInt retrieves int
// returns 0 if missing.
func (C *gtfsFileParser) getInt(name string, optional bool) int {
	result, err := getInt(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
	}
	if result == nil {
		return 0
	}
	return *result
}

// getIntPointer retrieves int pointer
// returns nil if missing.
func (C *gtfsFileParser) getIntPointer(name string, optional bool) *int {
	result, err := getInt(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
		return nil
	}
	return result
}

// getGTFSDatePointer retrieves date in gtfs format as time.Time pointer
// returns nil if missing
func (C *gtfsFileParser) getGTFSDatePointer(name string, optional bool) *time.Time {
	stringValue, err := findValue(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
		return nil
	}
	if stringValue == nil || len(*stringValue) == 0 && optional {
		return nil
	}
	result, err := timeFromYYYYMMDD(*stringValue)
	if err != nil {
		C.errors = append(C.errors, err)
		return nil
	}
	return &result
}

// getGTFSDate retrieves date in gtfs format
// returns default time.Time if missing
func (C *gtfsFileParser) getGTFSDate(name string, optional bool) time.Time {
	result := C.getGTFSDatePointer(name, optional)
	if result != nil {
		return *result
	}
	return time.Time{}
}

// getGTFSTime retrieves seconds since midnight in gtfs format from current row
// returns 0 if missing
func (C *gtfsFileParser) getGTFSTime(name string, optional bool) int {
	result := C.getGTFSTimePointer(name, optional)
	if result == nil {
		return 0
	}
	return *result
}

// getGTFSTimePointer retrieves seconds since midnight in gtfs format from current row
// returns nil if missing and optional is true
func (C *gtfsFileParser) getGTFSTimePointer(name string, optional bool) *int {
	result, err := getGTFSTime(name, C.currentRecords, C.headers, optional)
	if err != nil {
		C.errors = append(C.errors, err)
	}
	return result
}

// getError retrieve last error encountered while parsing csv file
func (C *gtfsFileParser) getError() error {
	if len(C.errors) > 0 {
		return fmt.Errorf("in file %v, line %v: %v", C.Filename, C.line, C.errors)
	}
	return nil
}

// addParseError appends error to list of parsing errors encountered in csv file
func (C *gtfsFileParser) addParseError(err error) {
	C.errors = append(C.errors, err)
}

// nextLine moves csvReader one line forward
func (C *gtfsFileParser) nextLine() error {
	var err error
	C.currentRecords, err = C.cvsReader.Read()
	C.line += 1
	return err
}

// find index of elements that matches name string. returns -1 if not found
func indexOf(name string, elements []string) int {
	for i, value := range elements {
		if name == value {
			return i
		}
	}
	return -1
}

// findValue retrieves string value from csv records
// returns nil if record isn't present and optional is true
func findValue(name string, records []string, headers []string, optional bool) (*string, error) {
	index := indexOf(name, headers)
	if index < 0 {
		if optional {
			return nil, nil
		}
		return nil, fmt.Errorf("unable to find header: %s", name)
	}
	if len(records) <= index {
		return nil, fmt.Errorf("records are too short to find header at %v named %s", index, name)
	}
	value := records[index]
	if len(value) == 0 && !optional {
		return nil, fmt.Errorf("missing required value in column %v", name)
	}
	return &value, nil
}

// getInt retrieves int from csv records
// returns nil if record isn't present and optional is true
func getInt(name string, records []string, headers []string, optional bool) (*int, error) {
	value, err := findValue(name, records, headers, optional)
	if err != nil || value == nil {
		return nil, err
	}
	if len(*value) == 0 {
		if optional {
			return nil, nil
		}
		return nil, fmt.Errorf("missing required value in column %v", name)
	}
	result, err := strconv.Atoi(*value)
	if err != nil {
		return nil, csvError(name, err)
	}
	return &result, nil
}

// getFloat64 retrieves float64 from csv records
// returns nil if record isn't present and optional is true
func getFloat64(name string, records []string, headers []string, optional bool) (*float64, error) {
	value, err := findValue(name, records, headers, optional)
	if err != nil || value == nil {
		return nil, err
	}
	if len(*value) == 0 {
		if optional {
			return nil, nil
		}
		return nil, fmt.Errorf("missing required value in column %v", name)
	}
	result, err := strconv.ParseFloat(*value, 64)
	if err != nil {
		return nil, csvError(name, err)
	}
	return &result, nil
}

// csvError convenience method for formatting an error and line number in csv file.
func csvError(name string, err error) error {
	return fmt.Errorf("unable to parse column %s, error: %v ", name, err)
}

// getGTFSTime retrieves gtfs seconds since midnight from records
func getGTFSTime(name string, records []string, headers []string, optional bool) (*int, error) {
	value, err := findValue(name, records, headers, optional)
	if err != nil || value == nil {
		return nil, err
	}
	//check for empty string
	str := strings.TrimSpace(*value)
	if len(str) == 0 { //empty string
		if optional {
			// it's ok that its empty
			return nil, nil
		}
		// it's not ok its empty
		return nil, fmt.Errorf("missing required value in column %v", name)

	}
	result, err := secondsFromGTFSTime(str)
	if err != nil {
		return result, csvError(name, err)
	}
	return result, nil
}

// secondsFromGTFSTime parses seconds of the schedule day from string defined in gtfs as :
// Time in the HH:MM:SS format (H:MM:SS is also accepted). The time is measured from "noon minus 12h" of the service day (effectively midnight except for days on which daylight savings time changes occur). For times occurring after midnight, enter the time as a value greater than 24:00:00 in HH:MM:SS local time for the day on which the trip schedule begins.
// Example: 14:30:00 for 2:30PM or 25:35:00 for 1:35AM on the next day.
func secondsFromGTFSTime(gtfsTime string) (*int, error) {
	parts := strings.Split(gtfsTime, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected three colons in Time format: %s", gtfsTime)
	}
	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	result := (hours * 60 * 60) + (minutes * 60) + seconds
	return &result, nil
}

// timeFromYYYYMMDD retrieves date from gtfs date formatted string:
// Service day in the YYYYMMDD format. Since time within a service day can be above 24:00:00, a service day often contains information for the subsequent day(s).
// Example: 20180913 for September 13th, 2018.
func timeFromYYYYMMDD(dateString string) (time.Time, error) {
	const layout = "20060102"
	result, err := time.Parse(layout, dateString)
	return result, err
}

// loadGTFSRows iterates over all rows in gtfsFileParser and feeds them into rowReader.
// reading halts if an error occurs and the error is returned
func loadGTFSRows(dsTx *gtfs.DataSetTransaction, parser *gtfsFileParser, rowReader gtfsRowReader) error {

	for {
		err := parser.nextLine()

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = rowReader.addRow(parser, dsTx)

		if err != nil {
			parser.addParseError(err)
			return parser.getError()
		}
	}
	//flush the remaining items out of the row reader into the database
	return rowReader.flush(dsTx)
}

// loadGtfsZipFile reads local zip file at localGTFSFilePath, uncompresses the files inside, if a gtfsRowReader
// is available for the file its used to read and record the file.
// reading halts if an error occurs and the error is returned.
// returns list of files that have been read.
func loadGtfsZipFile(log *log.Logger, gtfsDataSetTx *gtfs.DataSetTransaction, localGTFSFilePath string) ([]string, error) {

	fileList := make([]string, 0)
	r, err := zip.OpenReader(localGTFSFilePath)
	if err != nil {
		return fileList, err
	}
	//close the file after we are done
	defer func() {
		err := r.Close()
		if err != nil {
			log.Printf("unable to close zip file %s, error: %v", localGTFSFilePath, err)
		}
	}()

	//iterate over each file
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			//ignore folders
			continue
		}
		// time to log how long it took to read file
		start := time.Now()
		rowReader := getGTFSRowReader(f.Name)
		if rowReader == nil {
			//ignore
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return fileList, err
		}
		parser, err := makeGTFSFileParser(rc, f.Name)
		if err != nil {
			return fileList, err
		}
		log.Printf("Loading %s\n", parser.Filename)
		err = loadGTFSRows(gtfsDataSetTx, parser, rowReader)
		if err != nil {
			return fileList, err
		}
		err = rc.Close()
		if err != nil {
			return fileList, err
		}
		log.Printf("Loaded %d rows in file %s in %d seconds\n", parser.line, parser.Filename,
			time.Now().Unix()-start.Unix())

		fileList = append(fileList, f.Name)
	}
	return fileList, nil

}

// getGTFSRowReader returns gtfsRowReader for fileName
// returns nil if the file type is not read
func getGTFSRowReader(fileName string) gtfsRowReader {
	switch fileName {
	case "calendar.txt":
		return &calendarRowReader{}
	case "calendar_dates.txt":
		return &calendarDateRowReader{}
	case "trips.txt":
		return &tripRowReader{}
	case "stop_times.txt":
		return &stopTimeRowReader{}
	case "shapes.txt":
		return &shapeRowReader{}
	}
	return nil
}
