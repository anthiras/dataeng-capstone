"""ETL/query pipeline"""
import data, quality, queries

def run(spark):
    """Run the ETL/query pipeline"""

    # Load, preprocess and quality check flights data
    flights = data.load_flights(spark)
    print("Total flights: " + str(flights.count()))
    quality.verify_not_empty(flights)
    quality.verify_values(flights, "cancelled", [0, 1])
    flights = data.preprocess_flights(flights)
    quality.verify_no_missing_values(flights, "origin")
    quality.verify_no_missing_values(flights, "dest")

    # Load, preprocess and quality check airports data
    airports = data.load_airports(spark)
    print("Total airports: " + str(airports.count()))
    quality.verify_not_empty(airports)
    airports = data.preprocess_airports(airports)
    quality.verify_no_missing_values(airports, "iata_code")

    # Join data
    flights_and_airports = data.join(flights, airports)
    quality.verify_not_empty(flights_and_airports)

    # Run queries
    queries.delayed_routes(flights_and_airports).show(10)
    queries.airport_type_stats(flights_and_airports).show()
    queries.elevation_stats(flights_and_airports).show()
