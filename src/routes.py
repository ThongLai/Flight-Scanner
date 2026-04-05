TARGET_ROUTES = [
    # MAN routes
    {
        "origin": "MAN",
        "destination": "SGN",
        "origin_city": "Manchester",
        "dest_city": "Ho Chi Minh City",
        "distance_miles": 5940,
        "notes": "No direct. Connects via ME/EU hubs",
    },
    {
        "origin": "MAN",
        "destination": "HAN",
        "origin_city": "Manchester",
        "dest_city": "Hanoi",
        "distance_miles": 5590,
        "notes": "No direct. Connects via ME/EU hubs",
    },
    {
        "origin": "MAN",
        "destination": "BKK",
        "origin_city": "Manchester",
        "dest_city": "Bangkok",
        "distance_miles": 5890,
        "notes": "No direct. Connects via ME/EU hubs",
    },
    {
        "origin": "SGN",
        "destination": "MAN",
        "origin_city": "Ho Chi Minh City",
        "dest_city": "Manchester",
        "distance_miles": 5940,
        "notes": "Return leg",
    },
    {
        "origin": "HAN",
        "destination": "MAN",
        "origin_city": "Hanoi",
        "dest_city": "Manchester",
        "distance_miles": 5590,
        "notes": "Return leg",
    },
    {
        "origin": "BKK",
        "destination": "MAN",
        "origin_city": "Manchester",
        "dest_city": "Bangkok",
        "distance_miles": 5890,
        "notes": "Return leg",
    },
    # LHR routes
    {
        "origin": "LHR",
        "destination": "SGN",
        "origin_city": "London",
        "dest_city": "Ho Chi Minh City",
        "distance_miles": 5980,
        "notes": "Primary target. VN direct via HAN, plus Emirates/SQ/CA 1-stop",
    },
    {
        "origin": "LHR",
        "destination": "HAN",
        "origin_city": "London",
        "dest_city": "Hanoi",
        "distance_miles": 5630,
        "notes": "VN Airlines direct. Also via PEK, CAN, HKG, BKK",
    },
    # Regional hub route
    {
        "origin": "LHR",
        "destination": "BKK",
        "origin_city": "London",
        "dest_city": "Bangkok",
        "distance_miles": 5930,
        "notes": "BA/TG/EVA direct. Common connection point for SGN/HAN",
    },
    # Return directions
    {
        "origin": "SGN",
        "destination": "LHR",
        "origin_city": "Ho Chi Minh City",
        "dest_city": "London",
        "distance_miles": 5980,
        "notes": "Return leg. Pricing often asymmetric",
    },
    {
        "origin": "HAN",
        "destination": "LHR",
        "origin_city": "Hanoi",
        "dest_city": "London",
        "distance_miles": 5630,
        "notes": "Return leg",
    },
    {
        "origin": "BKK",
        "destination": "LHR",
        "origin_city": "Bangkok",
        "dest_city": "London",
        "distance_miles": 5930,
        "notes": "Return leg",
    },
]

# Routes to add later if budget allows
FUTURE_ROUTES = [
    {
        "origin": "LHR",
        "destination": "SIN",
        "origin_city": "London",
        "dest_city": "Singapore",
        "distance_miles": 6760,
        "notes": "SQ direct. Comparison long-haul route",
    },
    {
        "origin": "LHR",
        "destination": "DAD",
        "origin_city": "London",
        "dest_city": "Da Nang",
        "distance_miles": 5770,
        "notes": "Vietnam tourist route. No direct, always connecting",
    },
]

# Collection configuration
COLLECTION_CONFIG = {
    "currency": "GBP",
    "max_polls": 3,
    "poll_interval": 2.0,
}
