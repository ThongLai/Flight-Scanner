import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from ml.predict import predict_flight
from ml.window import find_best_dates
from ml.optimise import find_pareto_flights, _skyscanner_link  


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


class FlightBotHandler:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not self.bot_token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

    async def start_command(self, update: Update,
                            ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Flight Scanner Bot*\n\n"
            "ML-powered flight price prediction.\n\n"
            "*Commands:*\n"
            "/predict ROUTE YYYY-MM-DD\n"
            "/window ROUTE START END\n"
            "/help\n\n"
            "Example:\n/predict LHR-SGN 2026-09-15",
            parse_mode="Markdown",
        )

    async def help_command(self, update: Update,
                           ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Commands*\n\n"
            "/predict ROUTE YYYY-MM-DD\n"
            "  Price estimate + book/wait advice.\n"
            "  Example: /predict LHR-SGN 2026-09-15\n\n"
            "/window ROUTE START END\n"
            "  Cheapest dates in a travel window.\n"
            "  Example: /window LHR-SGN 2026-08-01 2026-09-30\n\n"
            "*Routes:* LHR/MAN <-> SGN/HAN/BKK (both directions)",
            parse_mode="Markdown",
        )

    async def predict_command(self, update: Update,
                              ctx: ContextTypes.DEFAULT_TYPE):
        try:
            route = ctx.args[0].upper()
            date_str = ctx.args[1]
        except (IndexError, ValueError):
            await update.message.reply_text(
                "Usage: /predict LHR-SGN 2026-09-15"
            )
            return

        try:
            r = predict_flight(route, date_str)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        msg = (
            f"*Price Prediction*\n\n"
            f"Route: {r['route']}\n"
            f"Departure: {r['departure_date']} "
            f"({r['days_ahead']} days away)\n\n"
            f"Estimated price: £{r['estimated_price']}\n"
            f"Last observed: £{r['last_observed_price']}\n\n"
            f"Recommendation: *{r['action']}* "
            f"({r['confidence']} confidence)"
        )
        if not r["has_history"]:
            msg += "\n\n_No recent history; using route average._"

        await update.message.reply_text(msg, parse_mode="Markdown")

    async def window_command(self, update: Update,
                             ctx: ContextTypes.DEFAULT_TYPE):
        try:
            route = ctx.args[0].upper()
            origin, dest = route.split("-")
            start, end = ctx.args[1], ctx.args[2]
        except (IndexError, ValueError):
            await update.message.reply_text(
                "Usage: /window LHR-SGN 2026-08-01 2026-09-30"
            )
            return

        await update.message.reply_text("Searching best dates...")

        try:
            best = find_best_dates(origin, dest, start, end)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        if best.empty:
            await update.message.reply_text("No prices found for that range.")
            return

        lines = [f"*Best dates {route}*\n"]
        for i, row in enumerate(best.itertuples(), 1):
            d = row.departure_date.strftime("%Y-%m-%d (%a)")
            lines.append(
                f"{i}. {d} -> £{row.price:.0f} "
                f"({row.action}, {row.confidence})"
            )
        await update.message.reply_text("\n".join(lines),
                                        parse_mode="Markdown")

    async def optimise_command(self, update: Update,
                              ctx: ContextTypes.DEFAULT_TYPE):
        """Handle /optimise ROUTE DATE - Pareto-optimal flights."""
        try:
            route = ctx.args[0].upper()
            origin, dest = route.split("-")
            search_date = ctx.args[1]
        except (IndexError, ValueError):
            await update.message.reply_text(
                "Usage: /optimise LHR-SGN 2026-09-15"
            )
            return

        await update.message.reply_text("Finding best flights...")

        try:
            pareto = find_pareto_flights(origin, dest, search_date)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        if pareto.empty:
            await update.message.reply_text(
                "No flights found for that route/date."
            )
            return

        source = pareto.attrs.get("source", "collected")
        link = _skyscanner_link(origin, dest, search_date)

        lines = [f"*Pareto-optimal flights {route}*",
                 f"_{search_date}_ (source: {source})\n"]
        for i, row in enumerate(pareto.itertuples(), 1):
            stops = "direct" if row.stop_count == 0 else f"{row.stop_count} stop(s)"
            lines.append(
                f"[{i}. £{row.price_raw:.0f}  {row.duration_hrs}h  {stops}]({link})\n"
                f"   {row.carrier_names}"
            )

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )



def main():
    handler = FlightBotHandler()
    app = Application.builder().token(handler.bot_token).build()
    app.add_handler(CommandHandler("start", handler.start_command))
    app.add_handler(CommandHandler("help", handler.help_command))
    app.add_handler(CommandHandler("predict", handler.predict_command))
    app.add_handler(CommandHandler("window", handler.window_command))
    app.add_handler(CommandHandler("optimise", handler.optimise_command))
    print("Bot running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
