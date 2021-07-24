package monitor

import "math"

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
		t = math.Min( 1, math.Max( 0, pointsDiffSquared /startEndDiffSquared) )
	}
	return startLat + pointEndLatDiff * t, startLon + pointEndLonDiff * t

}