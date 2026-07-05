import os
from telegram import Bot
from telegram.constants import ParseMode
import asyncio
import ast
from datetime import datetime


class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.bot = Bot(token=bot_token)
    
    async def send_message_async(self, message, parse_mode=ParseMode.MARKDOWN):
        """Send message asynchronously"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode,
                disable_web_page_preview=False
            )
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    def send_message(self, message, parse_mode=ParseMode.MARKDOWN):
        """Synchronous wrapper - only use when NOT in async context"""
        return asyncio.run(self.send_message_async(message, parse_mode))
    
    def parse_list(self, data):
        """Parse list from string or return as-is"""
        if isinstance(data, str):
            try:
                return ast.literal_eval(data)
            except:
                return []
        if isinstance(data, list):
            return data
        return []
    
    def format_time(self, timestamp):
        """Format timestamp to readable time"""
        if not timestamp:
            return "N/A"
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime("%d/%m %H:%M")
        except:
            return "N/A"
    
    def format_duration(self, minutes):
        """Convert minutes to hours and minutes"""
        if not minutes:
            return "N/A"
        hours = minutes // 60
        mins = minutes % 60
        if hours > 0 and mins > 0:
            return f"{hours}h {mins}m"
        elif hours > 0:
            return f"{hours}h"
        else:
            return f"{mins}m"
    
    def create_summary_table(self, flights_df, max_flights=5):
        """Create a clean summary table of top flights"""
        top_flights = flights_df.nsmallest(max_flights, 'price_gbp')
        
        lines = []
        
        for idx, (_, row) in enumerate(top_flights.iterrows(), 1):
            price = f"£{row['price_gbp']:.0f}"
            airline = row['out_airline'].split(',')[0][:12]  # First airline, max 12 chars
            route = f"{row['out_origin']}->{row['out_destination']}"
            depart_time = self.format_time(row['out_departure'])
            stops = row['total_stops']
            
            # Get transit airports
            out_airports = self.parse_list(row.get('out_transit_airports', []))
            via = ','.join(out_airports[:2]) if out_airports else 'Direct'  # Show max 2 airports
            if len(out_airports) > 2:
                via += '...'
            
            # Format line with consistent spacing
            line = f"{idx}. {price:>6} | {airline:<12} | {route:<10} | {depart_time:<11} | {stops} stop(s) | via {via}"
            lines.append(line)
        
        return '\n'.join(lines)
    
    def format_flight_notification(self, flight_data, search_params):
        """Format flight data into detailed notification"""
        
        price = flight_data['price_gbp']
        
        # Generate Skyscanner link
        from_code = search_params['from_code']
        to_code = search_params['to_code']
        depart_date = search_params['depart_date'].replace('-', '')
        return_date = search_params['return_date'].replace('-', '')
        
        skyscanner_link = (
            f"https://www.skyscanner.net/transport/flights/"
            f"{from_code.lower()}/{to_code.lower()}/"
            f"{depart_date[2:]}/{return_date[2:]}/"
            f"?adultsv2=1&cabinclass=economy"
        )
        
        # Parse lists
        out_airports = self.parse_list(flight_data.get('out_transit_airports', []))
        ret_airports = self.parse_list(flight_data.get('ret_transit_airports', []))
        out_layovers = self.parse_list(flight_data.get('out_layover_hours', []))
        ret_layovers = self.parse_list(flight_data.get('ret_layover_hours', []))
        out_flight_nums = self.parse_list(flight_data.get('out_flight_numbers', []))
        ret_flight_nums = self.parse_list(flight_data.get('ret_flight_numbers', []))
        
        # Trip length
        trip_days = flight_data.get('trip_length_days', 0)
        
        # Build message
        message = "*FLIGHT DEAL FOUND*\n\n"
        message += f"*Price:* £{price:.2f}\n"
        message += f"*Route:* {from_code} -> {to_code}\n"
        message += f"*Trip Length:* {trip_days} days\n"
        message += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Outbound flight
        message += f"*OUTBOUND FLIGHT*\n"
        message += f"  {flight_data['out_origin']} -> {flight_data['out_destination']}\n"
        message += f"  Depart: {self.format_time(flight_data['out_departure'])}\n"
        message += f"  Arrive: {self.format_time(flight_data['out_arrival'])}\n"
        message += f"  Duration: {self.format_duration(flight_data['out_duration_min'])}\n"
        message += f"  Airline: {flight_data['out_airline']}\n"
        
        if out_flight_nums:
            message += f"  Flight(s): {', '.join(out_flight_nums)}\n"
        
        if flight_data['out_stops'] == 0:
            message += f"  Direct flight\n"
        else:
            message += f"  Stops: {flight_data['out_stops']}\n"
            if out_airports:
                message += f"  Via: {' -> '.join(out_airports)}\n"
            if out_layovers:
                layover_str = ', '.join([f"{h}h" for h in out_layovers])
                message += f"  Layovers: {layover_str}\n"
        
        message += f"\n"
        
        # Return flight
        message += f"*RETURN FLIGHT*\n"
        message += f"  {flight_data['ret_origin']} -> {flight_data['ret_destination']}\n"
        message += f"  Depart: {self.format_time(flight_data['ret_departure'])}\n"
        message += f"  Arrive: {self.format_time(flight_data['ret_arrival'])}\n"
        message += f"  Duration: {self.format_duration(flight_data['ret_duration_min'])}\n"
        message += f"  Airline: {flight_data['ret_airline']}\n"
        
        if ret_flight_nums:
            message += f"  Flight(s): {', '.join(ret_flight_nums)}\n"
        
        if flight_data['ret_stops'] == 0:
            message += f"  Direct flight\n"
        else:
            message += f"  Stops: {flight_data['ret_stops']}\n"
            if ret_airports:
                message += f"  Via: {' -> '.join(ret_airports)}\n"
            if ret_layovers:
                layover_str = ', '.join([f"{h}h" for h in ret_layovers])
                message += f"  Layovers: {layover_str}\n"
        
        message += f"\n━━━━━━━━━━━━━━━━━━━━\n"
        message += f"[Book on Skyscanner]({skyscanner_link})\n"
        
        return message
    
    async def send_flight_deals(self, flights_df, search_params, max_flights=5):
        """Send top flight deals as notifications - Summary + Detailed Cards"""
        if flights_df.empty:
            message = "No flights found for your search."
            await self.send_message_async(message)
            return
        
        # Create summary with table
        current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        summary = "*FLIGHT DEAL ALERT*\n\n"
        summary += f"*Scan Time:* {current_time}\n"
        summary += f"*Route:* {search_params['from_code']} -> {search_params['to_code']}\n"
        summary += f"*Dates:* {search_params['depart_date']} to {search_params['return_date']}\n\n"
        summary += f"Found *{len(flights_df)}* flights\n"
        summary += f"Cheapest: *£{flights_df['price_gbp'].min():.2f}*\n"
        summary += f"Average: £{flights_df['price_gbp'].mean():.2f}\n"
        summary += f"━━━━━━━━━━━━━━━━━━━━\n"
        
        # Add summary table
        summary += f"\n*Top {max_flights} Deals:*\n"
        summary_table = self.create_summary_table(flights_df, max_flights)
        summary += f"```\n{summary_table}\n```\n"
        
        summary += f"\nSending detailed cards..."
        
        await self.send_message_async(summary, ParseMode.MARKDOWN)
        
        # Small delay before sending detailed cards
        await asyncio.sleep(2)
        
        # Send detailed cards for top flights
        top_flights = flights_df.nsmallest(max_flights, 'price_gbp')
        
        for idx, (_, row) in enumerate(top_flights.iterrows(), 1):
            row_dict = row.to_dict()
            flight_message = self.format_flight_notification(row_dict, search_params)
            flight_message = f"*Deal #{idx}*\n\n" + flight_message
            
            await self.send_message_async(flight_message, ParseMode.MARKDOWN)
            await asyncio.sleep(1)  # Delay between cards to avoid rate limits
