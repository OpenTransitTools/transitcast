package gtfs

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"strings"
	"time"
)

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
		"friday, " +
		"saturday, " +
		"sunday, " +
		"start_date," +
		"end_date) " +
		"values (" +
		":data_set_id, " +
		":service_id, " +
		":monday, " +
		":tuesday, " +
		":wednesday, " +
		":thursday, " +
		":friday, " +
		":saturday, " +
		":sunday, " +
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

// GetActiveServiceIds retrieves the active serviceIds on provided serviceDate.
// both calendar and calendar_date are used
func GetActiveServiceIds(db *sqlx.DB, dataSet *DataSet, serviceDate time.Time) ([]string, error) {
	serviceIdMap := make(map[string]bool)

	// the calendar week days columns are named after the english weekdays
	weekday := strings.ToLower(serviceDate.Weekday().String())

	query := fmt.Sprintf("select service_id from calendar where data_set_id = $1 "+
		"and $2 between start_date and end_date "+
		"and %s = 1", weekday)
	var calendarServiceKeys []string
	err := db.Select(&calendarServiceKeys, query, dataSet.Id, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve service_ids from calendar table. query:%s error: %w", query, err)
	}

	for _, serviceId := range calendarServiceKeys {
		serviceIdMap[serviceId] = true
	}

	var calendarDates []CalendarDate
	query = "select * from calendar_date where date = $1"
	err = db.Select(&calendarDates, query, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("unable to query calendar_date table. query:%s error: %w", query, err)
	}
	for _, calendarDate := range calendarDates {
		if calendarDate.ExceptionType == 1 {
			serviceIdMap[calendarDate.ServiceId] = true
		} else if calendarDate.ExceptionType == 2 {
			delete(serviceIdMap, calendarDate.ServiceId)
		}
	}

	return trueStringsFromMap(serviceIdMap), nil
}
