import pandas as pd
import requests
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

class FlightsScraperSky:
    def __init__(self, rapidapi_key):
        self.api_key = rapidapi_key
        self.base_url = "https://flights-sky.p.rapidapi.com"
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "flights-sky.p.rapidapi.com"
        }
    
    def search_airport(self, query):
        """Search for airports"""
        url = f"{self.base_url}/flights/auto-complete"
        
        params = {
            "query": query,
            "market": "UK",
            "locale": "en-GB"
        }
        
        print(f"{url} | Searching airports with query: {query}")
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error searching airports: {e}")
            return None
    
    def get_location_ids(self, city_name):
        """Get entityId for flight search"""
        results = self.search_airport(city_name)
        
        if not results or not results.get('status'):
            return None
        
        data = results.get('data', [])
        if not data:
            return None
        
        first = data[0]
        presentation = first.get('presentation', {})
        
        return {
            'entityId': presentation.get('id'),
            'skyId': presentation.get('skyId'),
            'name': presentation.get('title')
        }
    
    def search_roundtrip(self, from_entity_id, to_entity_id, depart_date, return_date):
        """Search for round-trip flights"""
        url = f"{self.base_url}/flights/search-roundtrip"
        
        params = {
            "fromEntityId": from_entity_id,
            "toEntityId": to_entity_id,
            "departDate": depart_date,
            "returnDate": return_date,
            "market": "UK",
            "locale": "en-GB",
            "currency": "GBP",
            "adults": "1",
            "cabinClass": "economy"
        }
        
        print(f"{url} | Searching roundtrip flights from {from_entity_id} to {to_entity_id}")
        
        try:
            print(f"\nSearching: {from_entity_id} -> {to_entity_id}")
            print(f"Dates: {depart_date} to {return_date}")
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error searching flights: {e}")
            return None
    
    def check_if_incomplete(self, results):
        """Check if results need polling"""
        if not results or not results.get('status'):
            return True
        
        context = results.get('data', {}).get('context', {})
        status = context.get('status', 'unknown')
        
        print(f"Status: {status}")
        return status == 'incomplete'
    
    def poll_incomplete_results(self, session_id):
        """Poll for complete results"""
        url = f"{self.base_url}/flights/search-incomplete"
        
        params = {
            "sessionId": session_id,
            "market": "UK",
            "locale": "en-GB",
            "currency": "GBP"
        }
        
        print(f"{url} | Polling for session ID: {session_id}")
        
        max_attempts = 10
        
        for attempt in range(max_attempts):
            print(f"Polling {attempt + 1}/{max_attempts}...")
            
            try:
                time.sleep(3)  # Wait before polling
                
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                results = response.json()
                
                if not self.check_if_incomplete(results):
                    print("Complete!")
                    return results
                
            except Exception as e:
                print(f"Polling error: {e}")
                return None
        
        print("Max attempts reached")
        return None

    def extract_transit_details(self, segments):
        """Extract transit country codes and layover durations"""
        if not segments or len(segments) <= 1:
            return {
                'countries': [],
                'airports': [],
                'layover_hours': []
            }
        
        countries = []
        airports = []
        layover_hours = []
        
        # Each connection point (except final destination)
        for i in range(len(segments) - 1):
            # Transit airport
            transit_airport = segments[i]['destination']['flightPlaceId']
            airports.append(transit_airport)
            
            # Transit country - get from parent country
            # Need to look this up from dictionaries
            # For now, we'll extract from the destination field if available
            dest_info = segments[i].get('destination', {})
            country = dest_info.get('country', 'UNKNOWN')
            countries.append(country)
            
            # Calculate layover
            arrival = segments[i]['arrival']
            next_departure = segments[i + 1]['departure']
            
            arr_dt = pd.to_datetime(arrival)
            dep_dt = pd.to_datetime(next_departure)
            layover = (dep_dt - arr_dt).total_seconds() / 3600
            layover_hours.append(round(layover, 1))
        
        return {
            'countries': countries,
            'airports': airports,
            'layover_hours': layover_hours
        }
      
    def parse_flights(self, results):
        """Parse flight results into pandas DataFrame"""
        if not results or not results.get('status'):
            return pd.DataFrame()
        
        itineraries = results.get('data', {}).get('itineraries', [])
        
        if not itineraries:
            return pd.DataFrame()
        
        flights_data = []
        
        for itin in itineraries:
            try:
                flight_id = itin.get('id')
                price = itin.get('price', {}).get('raw', 0)
                
                legs = itin.get('legs', [])
                
                # Outbound leg (first)
                outbound = legs[0] if len(legs) > 0 else {}
                outbound_segs = outbound.get('segments', [])
                
                # Return leg (second)
                return_leg = legs[1] if len(legs) > 1 else {}
                return_segs = return_leg.get('segments', [])
                
                # Extract transit countries and layovers
                outbound_transits = self.extract_transit_details(outbound_segs)
                return_transits = self.extract_transit_details(return_segs)
                
                # Extract flight numbers for each segment
                out_flight_numbers = []
                for seg in outbound_segs:
                    carriers = seg.get('operatingCarrier', {})
                    flight_num = carriers.get('flightNumber', '')
                    airline_code = carriers.get('iataCode', '')
                    if airline_code and flight_num:
                        out_flight_numbers.append(f"{airline_code}{flight_num}")
                
                ret_flight_numbers = []
                for seg in return_segs:
                    carriers = seg.get('operatingCarrier', {})
                    flight_num = carriers.get('flightNumber', '')
                    airline_code = carriers.get('iataCode', '')
                    if airline_code and flight_num:
                        ret_flight_numbers.append(f"{airline_code}{flight_num}")
                
                # Compile all data
                flight_row = {
                    'flight_id': flight_id,
                    'price_gbp': price,
                    
                    # Outbound info
                    'out_origin': outbound.get('origin', {}).get('displayCode'),
                    'out_destination': outbound.get('destination', {}).get('displayCode'),
                    'out_departure': outbound.get('departure'),
                    'out_arrival': outbound.get('arrival'),
                    'out_duration_min': outbound.get('durationInMinutes'),
                    'out_stops': outbound.get('stopCount'),
                    'out_airline': ', '.join([c.get('name', '') for c in outbound.get('carriers', {}).get('marketing', [])]),
                    'out_flight_numbers': out_flight_numbers,
                    'out_transit_countries': outbound_transits['countries'],
                    'out_transit_airports': outbound_transits['airports'],
                    'out_layover_hours': outbound_transits['layover_hours'],
                    
                    # Return info
                    'ret_origin': return_leg.get('origin', {}).get('displayCode'),
                    'ret_destination': return_leg.get('destination', {}).get('displayCode'),
                    'ret_departure': return_leg.get('departure'),
                    'ret_arrival': return_leg.get('arrival'),
                    'ret_duration_min': return_leg.get('durationInMinutes'),
                    'ret_stops': return_leg.get('stopCount'),
                    'ret_airline': ', '.join([c.get('name', '') for c in return_leg.get('carriers', {}).get('marketing', [])]),
                    'ret_flight_numbers': ret_flight_numbers,
                    'ret_transit_countries': return_transits['countries'],
                    'ret_transit_airports': return_transits['airports'],
                    'ret_layover_hours': return_transits['layover_hours'],
                    
                    # Additional
                    'total_duration_min': (outbound.get('durationInMinutes', 0) + return_leg.get('durationInMinutes', 0)),
                    'total_stops': outbound.get('stopCount', 0) + return_leg.get('stopCount', 0),
                }
                
                flights_data.append(flight_row)
                
            except Exception as e:
                print(f"Error parsing flight: {e}")
                continue
        
        df = pd.DataFrame(flights_data)
        
        # Add calculated columns
        if not df.empty:
            df['out_departure_date'] = pd.to_datetime(df['out_departure']).dt.date
            df['ret_departure_date'] = pd.to_datetime(df['ret_departure']).dt.date
            df['trip_length_days'] = (pd.to_datetime(df['ret_departure']) - 
                                    pd.to_datetime(df['out_departure'])).dt.days
        
        return df
