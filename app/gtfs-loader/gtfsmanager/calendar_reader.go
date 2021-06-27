package gtfsmanager

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
)

// calendarRowReader implements gtfsRowReader interface for gtfs.Calendar
type calendarRowReader struct {
}

func (r *calendarRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	calendar, err := buildCalendar(parser)
	if err != nil {
		return err
	}
	return gtfs.RecordCalendar(calendar, dsTx)
}

func (r *calendarRowReader) flush(_ *gtfs.DataSetTransaction) error {
	return nil
}

func buildCalendar(parser *gtfsFileParser) (*gtfs.Calendar, error) {
	calendar := gtfs.Calendar{
		ServiceId: parser.getString("service_id", false),
		Monday:    parser.getInt("monday", false),
		Tuesday:   parser.getInt("tuesday", false),
		Wednesday: parser.getInt("wednesday", false),
		Thursday:  parser.getInt("thursday", false),
		Friday:    parser.getInt("friday", false),
		Saturday:  parser.getInt("saturday", false),
		Sunday:    parser.getInt("sunday", false),
		StartDate: parser.getGTFSDatePointer("start_date", false),
		EndDate:   parser.getGTFSDatePointer("end_date", false),
	}

	return &calendar, parser.getError()
}
