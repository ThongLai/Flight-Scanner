import os
import logging
from datetime import datetime
import pandas as pd

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from ml.predict import predict_flight
from ml.window import find_best_dates
from ml.optimise import find_pareto_flights, _skyscanner_link  

import difflib
from telegram.ext import MessageHandler, filters


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

VALID_COMMANDS = ["start", "help", "predict", "window", "optimise"]

VALID_AIRPORTS = {"LHR", "MAN", "SGN", "HAN", "BKK"}


def validate_args(args, need_date=True, need_range=False):
    """Validate command args. Returns (parsed_dict, error_message)."""
    if not args:
        return None, "Missing arguments. See /help for usage."

    # Route
    route = args[0].upper()
    if "-" not in route:
        return None, (
            f"Route '{route}' looks wrong. Use ORIGIN-DEST "
            f"format, e.g. `LHR-SGN`."
        )
    origin, _, dest = route.partition("-")
    if origin not in VALID_AIRPORTS or dest not in VALID_AIRPORTS:
        valid = ", ".join(sorted(VALID_AIRPORTS))
        bad = origin if origin not in VALID_AIRPORTS else dest
        return None, (
            f"Airport '{bad}' not recognised.\n"
            f"Supported: {valid}"
        )

    parsed = {"route": route, "origin": origin, "dest": dest}

    # Dates
    def _check_date(s):
        try:
            d = pd.to_datetime(s, format="%Y-%m-%d")
            if d.normalize() < pd.Timestamp.now().normalize():
                return None, f"Date {s} is in the past."
            return d, None
        except (ValueError, TypeError):
            return None, (
                f"Date '{s}' is invalid. Use YYYY-MM-DD, "
                f"e.g. `2026-09-15`."
            )

    if need_range:
        if len(args) < 3:
            return None, "Need START and END dates. See /help."
        for key, val in [("start", args[1]), ("end", args[2])]:
            d, err = _check_date(val)
            if err:
                return None, err
            parsed[key] = val
        if pd.to_datetime(parsed["start"]) > pd.to_datetime(parsed["end"]):
            return None, "START date must be before END date."
    elif need_date:
        if len(args) < 2:
            return None, "Need a date. See /help."
        d, err = _check_date(args[1])
        if err:
            return None, err
        parsed["date"] = args[1]

    return parsed, None


def _fmt_time(ts) -> str:
    """Format an ISO timestamp to 'dd/mm HH:MM'."""
    if not ts or pd.isna(ts):
        return "N/A"
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime("%d/%m %H:%M")
    except (ValueError, TypeError):
        return "N/A"


def _fmt_duration(hrs: float) -> str:
    """Format decimal hours to 'Xh Ym'."""
    h = int(hrs)
    m = int(round((hrs - h) * 60))
    return f"{h}h {m}m" if m else f"{h}h"



class FlightBotHandler:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not self.bot_token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

    async def start_command(self, update: Update,
                            ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Flight Scanner Bot*\n\n"
            "ML-powered flight price prediction and optimisation.\n\n"
            "*Commands:*\n"
            "/predict ROUTE YYYY-MM-DD\n"
            "/window ROUTE START END\n"
            "/optimise ROUTE YYYY-MM-DD\n"
            "/help\n\n"
            "Type /help for detailed usage.\n\n"
            "_Maintained by Tom Lai_"
            "_Feedback: laiminhthong1@gmail.com_",
            parse_mode="Markdown",
        )

    async def help_command(self, update: Update,
                           ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Flight Scanner Bot*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "ML-powered flight price prediction and optimisation.\n\n"

            "*/window ROUTE START END*\n"
            "Ranks the most optimal departure dates in a range.\n"
            "`/window LHR-SGN 2026-09-15 2026-10-15`\n\n"

            "*/predict ROUTE DATE*\n"
            "Predicts whether the price will drop or rise on a date.\n"
            "`/predict LHR-SGN 2026-10-15`\n\n"

            "*/optimise ROUTE DATE*\n"
            "Finds Pareto-optimal live flights (price, duration, stops), "
            "each linked to Skyscanner.\n"
            "`/optimise LHR-SGN 2026-10-15`\n\n"

            "━━━━━━━━━━━━━━━━━━━━\n"
            "*Current Available Routes:* LHR/MAN <-> SGN/HAN/BKK (both directions)\n\n"

            "*Tip:* /window to find dates, /predict for price outlook, "
            "/optimise for bookable flights.\n\n",
            parse_mode="Markdown",
        )


    async def predict_command(self, update: Update,
                              ctx: ContextTypes.DEFAULT_TYPE):
        parsed, error = validate_args(ctx.args, need_date=True)
        if error:
            await update.message.reply_text(
                error + "\n\nExample: `/predict LHR-SGN 2026-10-15`",
                parse_mode="Markdown",
            )
            return

        try:
            r = predict_flight(parsed["route"], parsed["date"])
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        dep = pd.to_datetime(parsed["date"])
        dow = dep.strftime("%A")

        diff = r["estimated_price"] - r["last_observed_price"]
        if abs(diff) < 1:
            trend = "stable"
        elif diff > 0:
            trend = f"up £{abs(diff):.0f}"
        else:
            trend = f"down £{abs(diff):.0f}"

        prob_pct = r["wait_probability"] * 100
        thr_pct = r["wait_threshold"] * 100

        msg = (
            f"*Price Prediction — {r['route']}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Departure:* {dep.strftime('%d/%m/%Y')} ({dow})\n"
            f"*Days ahead:* {r['days_ahead']}\n\n"
            f"*Estimated lowest price:* £{r['estimated_price']:.0f}\n"
            f"*Last observed:* £{r['last_observed_price']:.0f}\n"
            f"*Trend:* {trend}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Outlook:* {r['action']}\n"
            f"*Confidence:* {r['confidence']} ({r['confidence_distance']:.2f})\n\n"
            f"_Model scores:_\n"
            f"  Price-drop probability: {prob_pct:.1f}%\n"
            f"  Decision threshold: {thr_pct:.0f}%\n"
            f"  Distance from threshold: {r['confidence_distance']:.2f}"
        )
        if not r["has_history"]:
            msg += "\n\n_No recent history; using route average._"

        await update.message.reply_text(msg, parse_mode="Markdown")

    async def window_command(self, update: Update,
                             ctx: ContextTypes.DEFAULT_TYPE):
        parsed, error = validate_args(ctx.args, need_date=False, need_range=True)
        if error:
            await update.message.reply_text(
                error + "\n\nExample: `/window LHR-SGN 2026-09-15 2026-10-15`",
                parse_mode="Markdown",
            )
            return

        origin, dest = parsed["origin"], parsed["dest"]
        route = parsed["route"]
        start, end = parsed["start"], parsed["end"]

        await update.message.reply_text("Searching optimal dates...")

        try:
            best = find_best_dates(origin, dest, start, end)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        if best.empty:
            await update.message.reply_text("No prices found for that range.")
            return

        start_fmt = pd.to_datetime(start).strftime("%d/%m/%Y")
        end_fmt = pd.to_datetime(end).strftime("%d/%m/%Y")
        top = best.iloc[0]

        lines = [
            f"*Optimal dates — {route}*",
            f"_{start_fmt} to {end_fmt}_\n",
            f"*Lowest:* £{top['price']:.0f} on "
            f"{top['departure_date'].strftime('%d/%m/%Y (%A)')}\n",
            "━━━━━━━━━━━━━━━━━━━━",
        ]

        for i, row in enumerate(best.itertuples(), 1):
            d_full = row.departure_date.strftime("%d/%m/%Y (%A)")
            d_link = _skyscanner_link(
                origin, dest, row.departure_date.strftime("%Y-%m-%d")
            )
            outlook = (
                "may drop" if "drop" in row.action.lower() else "may rise"
            )
            lines.append(
                f"*{i}. {d_full}* — £{row.price:.0f}\n"
                f"   Price {outlook} ({row.confidence}) · "
                f"[View flights]({d_link})"
            )

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    async def optimise_command(self, update: Update,
                              ctx: ContextTypes.DEFAULT_TYPE):
        parsed, error = validate_args(ctx.args, need_date=True)
        if error:
            await update.message.reply_text(
                error + "\n\nExample: `/optimise LHR-SGN 2026-10-15`",
                parse_mode="Markdown",
            )
            return

        origin, dest = parsed["origin"], parsed["dest"]
        route = parsed["route"]
        search_date = parsed["date"]

        await update.message.reply_text("Finding optimal flights...")

        try:
            pareto = find_pareto_flights(origin, dest, search_date)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return

        if pareto.empty:
            await update.message.reply_text(
                f"No flights found for {route} on {search_date}.\n"
                f"This route may not be served, or try a different date."
            )
            return

        dep_fmt = pd.to_datetime(search_date).strftime("%d/%m/%Y")
        lines = [f"*Pareto-optimal flights — {route}*",
                 f"_{dep_fmt}_\n"]

        for i, row in enumerate(pareto.itertuples(), 1):
            stops = "Direct" if row.stop_count == 0 else f"{row.stop_count} stop(s)"
            dur = _fmt_duration(row.duration_hrs)
            dep = _fmt_time(row.departure)
            arr = _fmt_time(row.arrival)
            route_path = row.segment_route or f"{origin} -> {dest}"
            link = _skyscanner_link(origin, dest, search_date,
                                    row.itinerary_id)

            lines.append(
                f"*{i}. £{row.price_raw:.0f}* — {stops}\n"
                f"   {row.carrier_names}\n"
                f"   Duration: {dur}\n"
                f"   Depart: {dep}  Arrive: {arr}\n"
                f"   Route: {route_path}\n"
                f"   [Book this flight]({link})"
            )

        await update.message.reply_text(
            "\n\n".join(lines),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    async def unknown_command(self, update: Update,
                              ctx: ContextTypes.DEFAULT_TYPE):
        """Handle unrecognised commands with a suggestion."""
        text = update.message.text.lstrip("/").split()[0].lower()
        match = difflib.get_close_matches(text, VALID_COMMANDS, n=1, cutoff=0.5)

        if match:
            await update.message.reply_text(
                f"Unknown command '/{text}'. Did you mean */{match[0]}*?\n"
                f"Type /help for all commands.",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                f"Unknown command '/{text}'.\nType /help to see what I can do."
            )


def main():
    handler = FlightBotHandler()
    app = Application.builder().token(handler.bot_token).build()
    app.add_handler(CommandHandler("start", handler.start_command))
    app.add_handler(CommandHandler("help", handler.help_command))
    app.add_handler(CommandHandler("predict", handler.predict_command))
    app.add_handler(CommandHandler("window", handler.window_command))
    app.add_handler(CommandHandler("optimise", handler.optimise_command))
    app.add_handler(MessageHandler(filters.COMMAND, handler.unknown_command))

    print("Bot running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
