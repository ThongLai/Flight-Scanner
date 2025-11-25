import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flights_scraper_sky import FlightsScraperSky
from telegram_notifier import TelegramNotifier
from test_flights_api import save_flights_to_json, load_flights_from_json

load_dotenv()


class FlightBotHandler:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.flights_api_key = os.getenv('FLIGHTS_API_KEY')
        
        if not all([self.bot_token, self.chat_id, self.flights_api_key]):
            raise ValueError("Missing required environment variables")
        
        self.notifier = TelegramNotifier(self.bot_token, self.chat_id)
        self.scanner = FlightsScraperSky(self.flights_api_key)
        
        # Default search parameters
        self.search_params = {
            'from_city': 'London',
            'to_city': 'Ho Chi Minh',
            'from_code': 'LON',
            'to_code': 'SGN',
            'depart_date': '2025-12-20',
            'return_date': '2026-01-10'
        }
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        welcome_message = (
            "*Welcome to Flight Scanner Bot*\n\n"
            "I can help you find the best flight deals.\n\n"
            "*Available Commands:*\n"
            "/search - Search for new flight deals\n"
            "/deals - Show deals from last search\n"
            "/status - Show current search settings\n"
            "/help - Show this help message\n\n"
            "-- Created and maintained by Tom Lai --"
        )
        await update.message.reply_text(welcome_message, parse_mode='Markdown')
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_message = (
            "*Flight Deal Hunter Commands:*\n\n"
            "*Search Commands:*\n"
            "/search - Fetch new flight data from API\n"
            "/deals [N] - Show top N deals (default: 5)\n"
            "  Example: /deals 3\n\n"
            "*Info Commands:*\n"
            "/status - Show current search settings\n"
            "/help - Show this help\n\n"
            "*Current Route:*\n"
            f"{self.search_params['from_code']} -> {self.search_params['to_code']}\n"
            f"Depart: {self.search_params['depart_date']}\n"
            f"Return: {self.search_params['return_date']}"
        )
        await update.message.reply_text(help_message, parse_mode='Markdown')
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        status_message = (
            "*Current Search Settings:*\n\n"
            f"*From:* {self.search_params['from_city']} ({self.search_params['from_code']})\n"
            f"*To:* {self.search_params['to_city']} ({self.search_params['to_code']})\n"
            f"*Departure:* {self.search_params['depart_date']}\n"
            f"*Return:* {self.search_params['return_date']}\n\n"
            "Use /search to fetch new deals"
        )
        await update.message.reply_text(status_message, parse_mode='Markdown')
    
    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /search command - fetch new flight data"""
        await update.message.reply_text(
            "*Starting flight search...*\n\nThis may take 30-60 seconds.",
            parse_mode='Markdown'
        )
        
        try:
            # Step 1: Get location IDs
            await update.message.reply_text("Step 1/3: Getting location IDs...")
            
            london = self.scanner.get_location_ids(self.search_params['from_city'])
            if not london:
                await update.message.reply_text("Error: Could not find origin location")
                return
            
            hcmc = self.scanner.get_location_ids(self.search_params['to_city'])
            if not hcmc:
                await update.message.reply_text("Error: Could not find destination location")
                return
            
            # Step 2: Search flights
            await update.message.reply_text("Step 2/3: Searching flights...")
            
            results = self.scanner.search_roundtrip(
                from_entity_id=london['entityId'],
                to_entity_id=hcmc['entityId'],
                depart_date=self.search_params['depart_date'],
                return_date=self.search_params['return_date']
            )
            
            if not results:
                await update.message.reply_text("Error: Failed to get flight results")
                return
            
            # Step 3: Poll if incomplete
            if self.scanner.check_if_incomplete(results):
                await update.message.reply_text("Step 3/3: Waiting for complete results...")
                session_id = results.get('data', {}).get('context', {}).get('sessionId')
                if session_id:
                    results = self.scanner.poll_incomplete_results(session_id)
            
            if not results:
                await update.message.reply_text("Error: Failed to get complete results")
                return
            
            # Parse results
            flights_df = self.scanner.parse_flights(results)
            
            if flights_df.empty:
                await update.message.reply_text("No flights found")
                return
            
            # Save to JSON
            save_flights_to_json(flights_df, 'flight_results.json')
            
            # FIXED: Send success message without parse_mode to avoid markdown issues
            cheapest = flights_df['price_gbp'].min()
            average = flights_df['price_gbp'].mean()
            
            success_msg = (
                "Search Complete!\n\n"
                f"Found {len(flights_df)} flights\n"
                f"Cheapest: £{cheapest:.2f}\n"
                f"Average: £{average:.2f}\n\n"
                f"Data saved to flight_results.json\n"
                f"Use /deals to see top deals"
            )
            await update.message.reply_text(success_msg)  # No parse_mode
            
        except Exception as e:
            error_msg = f"Error during search: {str(e)}"
            await update.message.reply_text(error_msg)
    
    async def deals_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /deals command - show deals from saved file"""
        # Get number of deals to show (default: 5)
        max_flights = 5
        if context.args:
            try:
                max_flights = int(context.args[0])
                max_flights = max(1, min(max_flights, 10))  # Between 1 and 10
            except ValueError:
                await update.message.reply_text("Invalid number. Using default (5).")
        
        await update.message.reply_text(
            f"*Loading deals...*\n\nShowing top {max_flights} deals",
            parse_mode='Markdown'
        )
        
        try:
            # Load from saved file
            flights_df = load_flights_from_json('flight_results.json')
            
            if flights_df.empty:
                await update.message.reply_text(
                    "No saved data found.\nUse /search to fetch new deals."
                )
                return
            
            # Send notifications
            await self.notifier.send_flight_deals(
                flights_df, 
                self.search_params, 
                max_flights=max_flights
            )
            
        except Exception as e:
            error_msg = f"Error loading deals: {str(e)}\n\nUse /search to fetch new data."
            await update.message.reply_text(error_msg)


def main():
    """Main function to run the bot"""
    print("="*60)
    print("STARTING TELEGRAM BOT")
    print("="*60)
    
    try:
        handler = FlightBotHandler()
        
        # Create application
        application = Application.builder().token(handler.bot_token).build()
        
        # Add command handlers
        application.add_handler(CommandHandler("start", handler.start_command))
        application.add_handler(CommandHandler("help", handler.help_command))
        application.add_handler(CommandHandler("status", handler.status_command))
        application.add_handler(CommandHandler("search", handler.search_command))
        application.add_handler(CommandHandler("deals", handler.deals_command))
        
        # Start the bot
        print("\nBot is running...")
        print("Press Ctrl+C to stop")
        print("\nAvailable commands:")
        print("  /start  - Welcome message")
        print("  /search - Fetch new flight data")
        print("  /deals  - Show deals from last search")
        print("  /status - Show current settings")
        print("  /help   - Show help message")
        print()
        
        # Run the bot
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure your .env file has:")
        print("  - TELEGRAM_BOT_TOKEN")
        print("  - TELEGRAM_CHAT_ID")
        print("  - FLIGHTS_API_KEY")


if __name__ == "__main__":
    main()
