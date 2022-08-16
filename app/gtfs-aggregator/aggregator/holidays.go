package aggregator

import (
	"github.com/rickar/cal/v2"
	"github.com/rickar/cal/v2/us"
	"time"
)

//transitHolidayCalendar holds the holidays observed by a transit agency, used to populate the holiday model feature
type transitHolidayCalendar struct {
	calendar *cal.BusinessCalendar
}

//makeTransitHolidayCalendar builds transitHolidayCalendar
//TODO:: should be customizable by transit agency rather than being hardcoded as it is now.
func makeTransitHolidayCalendar() *transitHolidayCalendar {
	calendar := cal.NewBusinessCalendar()
	calendar.AddHoliday(
		us.NewYear,
		us.MlkDay,
		us.MemorialDay,
		us.IndependenceDay,
		us.LaborDay,
		us.ThanksgivingDay,
		us.ChristmasDay,
		us.Juneteenth,
	)
	return &transitHolidayCalendar{calendar: calendar}
}

//isHoliday returns true if at is on a holiday observed by the transit agency, currently hard coded
func (t *transitHolidayCalendar) isHoliday(at time.Time) bool {
	_, observed, _ := t.calendar.IsHoliday(at)
	return observed

}
