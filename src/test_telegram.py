import os
from dotenv import load_dotenv
from telegram_notifier import TelegramNotifier

load_dotenv()

def test_simple_message():
    """Test 1: Send a simple message"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("Error: Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env")
        return
    
    notifier = TelegramNotifier(bot_token, chat_id)
    
    message = "*Test Message*\n\nYour FlightDealHunter bot is working!"
    
    print("Sending test message...")
    success = notifier.send_message(message)
    
    if success:
        print("Success! Check your Telegram app.")
    else:
        print("Failed to send message.")


def test_flight_notification():
    """Test 2: Send a formatted flight deal"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    notifier = TelegramNotifier(bot_token, chat_id)
    
    # Sample flight data
    sample_flight = {
        'price_gbp': 1428.22,
        'out_origin': 'LHR',
        'out_destination': 'SGN',
        'out_airline': 'Air China',
        'out_duration_min': 1305,
        'out_stops': 1,
        'out_transit_airports': ['PEK'],
        'ret_origin': 'SGN',
        'ret_destination': 'LHR',
        'ret_airline': 'Air China',
        'ret_duration_min': 1400,
        'ret_stops': 1,
        'ret_transit_airports': ['PEK']
    }
    
    search_params = {
        'from_code': 'LON',
        'to_code': 'SGN',
        'depart_date': '2025-12-20',
        'return_date': '2026-01-10'
    }
    
    message = notifier.format_flight_notification(sample_flight, search_params)
    
    print("Sending flight notification...")
    success = notifier.send_message(message)
    
    if success:
        print("Success! Check your Telegram for the flight deal.")


if __name__ == "__main__":
    print("="*60)
    print("TESTING TELEGRAM BOT")
    print("="*60)
    
    print("\nTest 1: Simple Message")
    test_simple_message()
    
    print("\nTest 2: Flight Deal Notification")
    test_flight_notification()
