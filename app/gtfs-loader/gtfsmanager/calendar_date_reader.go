package gtfsmanager

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
)

// calendarRowReader implements gtfsRowReader interface for gtfs.CalendarDate
type calendarDateRowReader struct{}

func (c calendarDateRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	calendarDate, err := buildCalendarDate(parser)
	if err != nil {
		return err
	}
	return gtfs.RecordCalendarDate(calendarDate, dsTx)
}

func (c calendarDateRowReader) flush(_ *gtfs.DataSetTransaction) error {
	return nil
}

func buildCalendarDate(parser *gtfsFileParser) (*gtfs.CalendarDate, error) {
	calendarDate := gtfs.CalendarDate{
		ServiceId:     parser.getString("service_id", false),
		Date:          parser.getGTFSDate("date", false),
		ExceptionType: parser.getInt("exception_type", false),
	}

	return &calendarDate, parser.getError()
}
