package monitor

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"math"
)

//findTripDistanceOfVehicleFromPosition if possible find how far along the pattern a vehicle is from tripStopPosition.
//requires that tripStopPosition contain longitude and latitude
//and gtfs.StopTimeInstance to have ShapeDistTraveled populated
//and gtfs.Shape to have ShapeDistTraveled populated
func findTripDistanceOfVehicleFromPosition(position *tripStopPosition) *float64 {
	//if coordinates are not present can't continue
	if position.latitude == nil || position.longitude == nil {
		return nil
	}
	//if distances or trip shapes are not present can't continue
	if position.previousSTI.ShapeDistTraveled == nil ||
		position.nextSTI.ShapeDistTraveled == nil ||
		len(position.tripInstance.Shapes) == 0 {
		return nil
	}
	//if the vehicle is at the stop no need to do a calculation using the pattern
	if position.atPreviousStop {
		return position.previousSTI.ShapeDistTraveled
	}
	shapes := position.tripInstance.ShapesBetweenDistances(*position.previousSTI.ShapeDistTraveled, *position.nextSTI.ShapeDistTraveled)
	return findLineDistanceInFeet(float64(*position.latitude), float64(*position.longitude), shapes)

}

//findLineDistanceInFeet finds a location close to line segments from shapes and returns the distance
// along the pattern that location is on the pattern
func findLineDistanceInFeet(lat, lon float64, shapes []*gtfs.Shape) *float64 {
	var bestStart *gtfs.Shape
	var bestSnappedLat float64
	var bestSnappedLon float64
	bestLineDistance := 200.0 //don't find anything if the location is 200 meters off
	for i, end := range shapes {
		if i == 0 {
			continue
		}
		start := shapes[i-1]
		snappedLat, snappedLon := nearestLatLngToLineFromPoint(start.ShapePtLat, start.ShapePtLng,
			end.ShapePtLat, end.ShapePtLng, lat, lon)
		distance := simpleLatLngDistance(snappedLat, snappedLon, lat, lon)
		if distance < bestLineDistance {
			bestLineDistance = distance
			bestStart = start
			bestSnappedLat = snappedLat
			bestSnappedLon = snappedLon
		}
	}
	if bestStart == nil {
		return nil
	}
	//take the best snapped point and measure how far from the start of the line it is
	distanceFromPatternStart := simpleLatLngDistance(bestStart.ShapePtLat, bestStart.ShapePtLng, bestSnappedLat, bestSnappedLon)
	//convert to feet
	distanceFromPatternStart = distanceFromPatternStart * 3.281
	//add distance from start to the shape distance traveled to get the distance along the pattern this point is
	result := *bestStart.ShapeDistTraveled + distanceFromPatternStart
	return &result
}

//simpleLatLngDistance calculates the approximate distance between two pairs of coordinates with simplistic
//calculation of longitudinal distance based on latitudes.
//provides adequately accurate results for coordinates that are close together (in the same transit area)
//will not produce good results work for locations where longitude rolls over from -179.9 to 179.9
//returns distance in METERS
func simpleLatLngDistance(lat1, lon1, lat2, lon2 float64) float64 {
	//take average latitude and convert to radians
	lat := lat1 + lat2
	if lat != 0 { // don't divide by zero
		lat = (lat / 2) * 0.01745329
	}

	diffLat := 111300 * (lat1 - lat2)
	// at equator one degree is 111300 meters, use average latitude to convert
	diffLon := 111300 * math.Cos(lat) * (lon1 - lon2)

	return math.Sqrt((diffLon * diffLon) + (diffLat * diffLat))
}

//nearestLatLngToLineFromPoint calculates the approximate nearest point on a line from startLat, startLng to
//endLat,endLon from pointLat, pointLon
//will not produce good results work for locations where longitude rolls over from -179.9 to 179.9
//results should be close enough for coordinates that are close together (in the same transit area)
//returns resulting latitude and longitude
func nearestLatLngToLineFromPoint(startLat, startLon, endLat, endLon, pointLat, pointLon float64) (float64, float64) {
	pointXStartLonDiff := pointLon - startLon
	pointYStartLatDiff := pointLat - startLat
	pointEndLonDiff := endLon - startLon
	pointEndLatDiff := endLat - startLat
	startEndDiffSquared := (pointEndLonDiff * pointEndLonDiff) + (pointEndLatDiff * pointEndLatDiff)
	t := 0.0
	if startEndDiffSquared > 0 {
		pointsDiffSquared := pointXStartLonDiff*pointEndLonDiff + pointYStartLatDiff*pointEndLatDiff
		t = math.Min(1, math.Max(0, pointsDiffSquared/startEndDiffSquared))
	}
	return startLat + pointEndLatDiff*t, startLon + pointEndLonDiff*t

}
