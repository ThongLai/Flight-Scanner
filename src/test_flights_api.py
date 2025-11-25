import os
import asyncio
import pandas as pd
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from flights_scraper_sky import FlightsScraperSky
from telegram_notifier import TelegramNotifier


load_dotenv()

def test_api():
    """Test the API with correct structure"""
    api_key = os.getenv('FLIGHTS_API_KEY')
    
    if not api_key:
        print("Error: FLIGHTS_API_KEY not in .env")
        return None, None
    
    print("="*60)
    print("TESTING FLIGHTS SCRAPER SKY API")
    print("="*60)
    
    scanner = FlightsScraperSky(api_key)
    
    print("\nStep 1: Get London location ID...")
    london = scanner.get_location_ids("London")
    
    if not london:
        print("Failed to get London")
        return None, None
    
    print(f"Success: {london['name']} (entityId: {london['entityId']})")
    
    print("\nStep 2: Get Ho Chi Minh location ID...")
    hcmc = scanner.get_location_ids("Ho Chi Minh")
    
    if not hcmc:
        print("Failed to get Ho Chi Minh")
        return None, None
    
    print(f"Success: {hcmc['name']} (entityId: {hcmc['entityId']})")
    
    print("\nStep 3: Search flights...")
    results = scanner.search_roundtrip(
        from_entity_id=london['entityId'],
        to_entity_id=hcmc['entityId'],
        depart_date="2025-12-20",
        return_date="2026-01-10"
    )
    
    if not results:
        print("Failed to get results")
        return None, None
    
    if scanner.check_if_incomplete(results):
        print("\nResults incomplete, polling...")
        session_id = results.get('data', {}).get('context', {}).get('sessionId')
        if session_id:
            results = scanner.poll_incomplete_results(session_id)
        else:
            print("No session ID found!")
            return None, None
    
    if not results:
        print("Failed to get complete results")
        return None, None
    
    return scanner, results


def display_flights(flights_df):
    """Display flight statistics"""
    print("\n" + "="*60)
    print("FLIGHT STATISTICS")
    print("="*60)
    
    print(f"\nTotal flights: {len(flights_df)}")
    print(f"Price range: GBP {flights_df['price_gbp'].min():.2f} - {flights_df['price_gbp'].max():.2f}")
    print(f"Average price: GBP {flights_df['price_gbp'].mean():.2f}")

    # Show top 5 cheapest
    print("\n=== TOP 5 CHEAPEST ===")
    top_5 = flights_df.nsmallest(5, 'price_gbp')[
        ['price_gbp', 'out_airline', 'out_transit_airports', 'ret_transit_airports', 'total_stops']
    ]
    print(top_5.to_string(index=False))


def save_flights_to_json(flights_df, filename='flight_results.json'):
    """Save flights DataFrame to JSON file"""
    # Convert DataFrame to list of dictionaries
    flights_list = flights_df.to_dict('records')
    
    # Create metadata
    data = {
        'metadata': {
            'saved_at': datetime.now().isoformat(),
            'total_flights': len(flights_df),
            'cheapest_price': float(flights_df['price_gbp'].min()),
            'average_price': float(flights_df['price_gbp'].mean())
        },
        'flights': flights_list
    }
    
    # Save to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\nData saved to {filename}")
    print(f"Saved {len(flights_df)} flights")


def load_flights_from_json(filename='flight_results.json'):
    """Load flights from JSON file into DataFrame"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract metadata
        metadata = data.get('metadata', {})
        print(f"\nLoading data from {filename}")
        print(f"Saved at: {metadata.get('saved_at', 'Unknown')}")
        print(f"Total flights: {metadata.get('total_flights', 0)}")
        
        # Convert flights list back to DataFrame
        flights_list = data.get('flights', [])
        flights_df = pd.DataFrame(flights_list)
        
        print(f"Loaded {len(flights_df)} flights")
        
        return flights_df
    
    except FileNotFoundError:
        print(f"Error: File {filename} not found")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return pd.DataFrame()


async def send_notifications(flights_df, max_flights=5):
    """Async function to send notifications"""
    if flights_df.empty:
        print("No flights to send")
        return
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("Error: Telegram credentials not found in .env")
        return
    
    print("\n" + "="*60)
    print(f"SENDING TELEGRAM NOTIFICATIONS")
    print("="*60)
    
    notifier = TelegramNotifier(bot_token, chat_id)
    
    search_params = {
        'from_code': 'LON',
        'to_code': 'SGN',
        'depart_date': '2025-12-20',
        'return_date': '2026-01-10'
    }
    
    await notifier.send_flight_deals(flights_df, search_params, max_flights=max_flights)
    print("Notifications sent!")


def main():
    """Main function with command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Flight Deal Hunter - Search flights and send notifications',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Fetch new data and send notifications:
    python src/test_flights_api.py --mode fetch
    
  Load from file and send top 3 deals:
    python src/test_flights_api.py --mode load --max-flights 3
    
  Fetch without notifications:
    python src/test_flights_api.py --mode fetch --no-notify
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['fetch', 'load'],
        default='fetch',
        help='Mode: "fetch" to get new data from API, "load" to use saved JSON file'
    )
    
    parser.add_argument(
        '--file',
        default='flight_results.json',
        help='JSON file path (default: flight_results.json)'
    )
    
    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='Skip sending Telegram notifications'
    )
    
    parser.add_argument(
        '--max-flights',
        type=int,
        default=5,
        help='Number of top flights to send (default: 5)'
    )
    
    args = parser.parse_args()
    
    flights_df = pd.DataFrame()
    
    if args.mode == 'fetch':
        print("\nMode: FETCH - Retrieving new data from API")
        print("-" * 60)
        
        scanner, results = test_api()
        
        if scanner and results:
            print("\nParsing results...")
            flights_df = scanner.parse_flights(results)
            
            if not flights_df.empty:
                display_flights(flights_df)
                save_flights_to_json(flights_df, args.file)
            else:
                print("No flights found")
                return
        else:
            print("Failed to fetch data from API")
            return
    
    elif args.mode == 'load':
        print("\nMode: LOAD - Loading data from saved file")
        print("-" * 60)
        
        flights_df = load_flights_from_json(args.file)
        
        if not flights_df.empty:
            display_flights(flights_df)
        else:
            print("Failed to load data")
            return
    
    # Send notifications if not disabled
    if not args.no_notify and not flights_df.empty:
        asyncio.run(send_notifications(flights_df, max_flights=args.max_flights))
    else:
        if args.no_notify:
            print("\nNotifications disabled (--no-notify flag)")


if __name__ == "__main__":
    main()