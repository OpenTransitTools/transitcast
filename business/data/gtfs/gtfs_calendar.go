package gtfs

import "time"

// Calendar contains data from a record in a gtfs calendar.txt file
type Calendar struct {
	DataSetId int64  `db:"data_set_id"`
	ServiceId string `db:"service_id"`
	Monday    int
	Tuesday   int
	Wednesday int
	Thursday  int
	Friday    int
	Saturday  int
	Sunday    int
	StartDate *time.Time `db:"start_date"`
	EndDate   *time.Time `db:"end_date"`
}

// CalendarDate contains data from a record in a gtfs calendar_dates.txt file
type CalendarDate struct {
	DataSetId     int64  `db:"data_set_id"`
	ServiceId     string `db:"service_id"`
	Date          time.Time
	ExceptionType int `db:"exception_type"`
}

func RecordCalendar(calendar *Calendar, dsTx *DataSetTransaction) error {
	calendar.DataSetId = dsTx.DS.Id
	statementString := "insert into calendar ( " +
		"data_set_id, " +
		"service_id, " +
		"monday, " +
		"tuesday, " +
		"wednesday, " +
		"thursday, " +
		"friday," +
		"saturday," +
		"start_date," +
		"end_date) " +
		"values (" +
		":data_set_id, " +
		":service_id, " +
		":monday, " +
		":tuesday, " +
		":wednesday, " +
		":thursday, " +
		":friday," +
		":saturday," +
		":start_date," +
		":end_date) "
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, calendar)
	return err

}

func RecordCalendarDate(calendarDate *CalendarDate, dsTx *DataSetTransaction) error {
	calendarDate.DataSetId = dsTx.DS.Id
	statementString := "insert into calendar_date ( " +
		"data_set_id, " +
		"service_id, " +
		"date, " +
		"exception_type) " +
		"values (" +
		":data_set_id, " +
		":service_id, " +
		":date, " +
		":exception_type)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, calendarDate)
	return err

}
