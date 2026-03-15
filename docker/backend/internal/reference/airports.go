package reference

import (
	"bytes"
	_ "embed"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

//go:embed airport_locations.csv
var airportLocationsData []byte

//go:embed airports.dat
var airportsData []byte

type Airport struct {
	Code        string
	City        string
	State       string
	CountryName string
	ISOCountry  string
	ISORegion   string
	Lat         float64
	Lng         float64
}

var (
	loadAirportsOnce sync.Once
	airportIndex     map[string]Airport
	locationIndex    map[string]Airport
	airportIndexErr  error
)

func LookupAirport(code string) (Airport, error) {
	loadAirportsOnce.Do(loadAirports)
	if airportIndexErr != nil {
		return Airport{}, airportIndexErr
	}

	code = strings.ToUpper(strings.TrimSpace(code))
	airport, ok := airportIndex[code]
	if !ok {
		return Airport{}, fmt.Errorf("unknown airport code %q", code)
	}

	return airport, nil
}

func LookupLocation(city string, state string, countryName string) (Airport, error) {
	loadAirportsOnce.Do(loadAirports)
	if airportIndexErr != nil {
		return Airport{}, airportIndexErr
	}

	location, ok := locationIndex[locationKey(city, state, countryName)]
	if !ok {
		return Airport{}, fmt.Errorf("unknown location %q, %q, %q", city, state, countryName)
	}

	return location, nil
}

func loadAirports() {
	reader := csv.NewReader(bytes.NewReader(airportLocationsData))
	records, err := reader.ReadAll()
	if err != nil {
		airportIndexErr = err
		return
	}
	if len(records) == 0 {
		airportIndexErr = fmt.Errorf("airport location reference is empty")
		return
	}

	index := make(map[string]Airport, len(records)-1)
	locations := make(map[string]Airport, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 6 {
			continue
		}

		code := strings.ToUpper(strings.TrimSpace(record[0]))
		if len(code) != 3 {
			continue
		}

		airport := Airport{
			Code:        code,
			City:        strings.TrimSpace(record[1]),
			State:       strings.TrimSpace(record[2]),
			CountryName: strings.TrimSpace(record[3]),
			ISOCountry:  strings.TrimSpace(record[4]),
			ISORegion:   strings.TrimSpace(record[5]),
		}
		index[code] = airport
		key := locationKey(airport.City, airport.State, airport.CountryName)
		if _, exists := locations[key]; !exists {
			locations[key] = airport
		}
	}

	dataReader := csv.NewReader(bytes.NewReader(airportsData))
	dataReader.FieldsPerRecord = -1
	dataRecords, err := dataReader.ReadAll()
	if err != nil {
		airportIndexErr = err
		return
	}

	for _, record := range dataRecords {
		if len(record) < 8 {
			continue
		}

		code := strings.ToUpper(strings.TrimSpace(record[4]))
		if len(code) != 3 || code == `\N` {
			continue
		}

		lat, err := strconv.ParseFloat(strings.TrimSpace(record[6]), 64)
		if err != nil {
			continue
		}
		lng, err := strconv.ParseFloat(strings.TrimSpace(record[7]), 64)
		if err != nil {
			continue
		}

		airport := index[code]
		airport.Code = code
		if airport.City == "" {
			airport.City = strings.TrimSpace(record[2])
		}
		if airport.CountryName == "" {
			airport.CountryName = strings.TrimSpace(record[3])
		}
		airport.Lat = lat
		airport.Lng = lng
		index[code] = airport
	}

	airportIndex = index
	locationIndex = locations
}

func locationKey(city string, state string, countryName string) string {
	return strings.ToLower(strings.TrimSpace(city)) + "|" +
		strings.ToLower(strings.TrimSpace(state)) + "|" +
		strings.ToLower(strings.TrimSpace(countryName))
}
