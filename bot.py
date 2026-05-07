"""
ForexPulse Bot — Telegram trader news notification bot
Sources: Forex Factory (economic calendar) + Breaking news (BBC, Al Jazeera, Fox, Twitter/X)
"""

import asyncio
import logging
import re
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup
from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────
# CONFIGURATION — edit these values
# ─────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@YourChannelUsername")  # or a numeric chat id
TWITTER_BEARER = os.getenv("TWITTER_BEARER_TOKEN", "")   # optional, for Twitter/X v2 API

# Alert settings
PRE_EVENT_MINUTES = 30          # How many minutes before event to send reminder
IMPACT_FILTER = ["High"]        # Options: ["High", "Medium", "Low"] — add all you want
CHECK_CALENDAR_EVERY = 300      # seconds (5 min)
CHECK_NEWS_EVERY = 120          # seconds (2 min)
TWITTER_ACCOUNTS = [            # Accounts to monitor for breaking news
    "federalreserve",
    "ecb",
    "bankofengland",
    "BIS_org",
    "IMFNews",
]
TWITTER_KEYWORDS = ["rate hike", "rate cut", "inflation", "CPI", "NFP", "FOMC", "GDP", "recession"]

# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────── State ────────────────────────────────
alerted_events: set = set()          # event ids already pre-alerted
released_events: set = set()         # event ids already posted as released
seen_news_urls: set = set()          # breaking news urls already sent
seen_tweet_ids: set = set()          # tweet ids already sent
subscribers: set = set()             # user chat_ids who started the bot


# ═══════════════════════════════════════════
#  FOREX FACTORY SCRAPER
# ═══════════════════════════════════════════
async def fetch_forex_factory() -> list[dict]:
    """Scrape today's economic calendar from Forex Factory."""
    url = "https://www.forexfactory.com/calendar"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    events = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning(f"Forex Factory returned {resp.status}")
                    return []
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr.calendar__row")
        current_time_str = ""

        for row in rows:
            # time cell — sometimes carries over from previous row
            time_cell = row.select_one(".calendar__time")
            if time_cell and time_cell.get_text(strip=True):
                current_time_str = time_cell.get_text(strip=True)

            currency = row.select_one(".calendar__currency")
            event_cell = row.select_one(".calendar__event-title")
            impact_cell = row.select_one(".calendar__impact span")
            forecast_cell = row.select_one(".calendar__forecast")
            previous_cell = row.select_one(".calendar__previous")
            actual_cell = row.select_one(".calendar__actual")

            if not (currency and event_cell):
                continue

            impact_class = ""
            if impact_cell:
                classes = impact_cell.get("class", [])
                if "icon--ff-impact-red" in classes:
                    impact_class = "High"
                elif "icon--ff-impact-ora" in classes:
                    impact_class = "Medium"
                elif "icon--ff-impact-yel" in classes:
                    impact_class = "Low"

            if IMPACT_FILTER and impact_class not in IMPACT_FILTER:
                continue

            event_id = f"{current_time_str}_{event_cell.get_text(strip=True)}_{currency.get_text(strip=True)}"
            actual = actual_cell.get_text(strip=True) if actual_cell else ""

            events.append({
                "id": event_id,
                "time": current_time_str,
                "currency": currency.get_text(strip=True),
                "event": event_cell.get_text(strip=True),
                "impact": impact_class,
                "forecast": forecast_cell.get_text(strip=True) if forecast_cell else "—",
                "previous": previous_cell.get_text(strip=True) if previous_cell else "—",
                "actual": actual,
            })

    except Exception as e:
        logger.error(f"Forex Factory scrape error: {e}")

    return events


def parse_event_time(time_str: str) -> Optional[datetime]:
    """Parse Forex Factory time strings like '8:30am' into today's datetime (UTC)."""
    time_str = time_str.lower().strip()
    if not time_str or time_str in ("all day", ""):
        return None
    try:
        t = datetime.strptime(time_str, "%I:%M%p")
        now = datetime.now(timezone.utc)
        return now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    except ValueError:
        return None


# ═══════════════════════════════════════════
#  RSS NEWS SCRAPERS
# ═══════════════════════════════════════════
RSS_FEEDS = {
    "BBC News": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "Fox Business": "https://feeds.foxbusiness.com/foxbusiness/latest",
    "Reuters": "https://feeds.reuters.com/reuters/businessNews",
    "Bloomberg": "https://feeds.bloomberg.com/markets/news.rss",
}

FOREX_KEYWORDS = [
    "fed", "federal reserve", "rate", "inflation", "cpi", "nfp", "jobs",
    "gdp", "recession", "fomc", "ecb", "bank of england", "boe", "boj",
    "dollar", "euro", "sterling", "yen", "forex", "fx", "currency",
    "oil", "opec", "gold", "market", "economy", "economic", "trade",
    "tariff", "sanctions", "debt", "bond", "yield", "treasury",
]


async def fetch_rss(source_name: str, rss_url: str) -> list[dict]:
    """Fetch and parse an RSS feed, filter for forex-relevant articles."""
    articles = []
    headers = {"User-Agent": "ForexPulseBot/1.0"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(rss_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return []
                xml = await resp.text()

        soup = BeautifulSoup(xml, "xml")
        items = soup.find_all("item")

        for item in items[:15]:  # check latest 15
            title = item.find("title")
            link = item.find("link")
            pub_date = item.find("pubDate")

            if not (title and link):
                continue

            title_text = title.get_text(strip=True)
            link_text = link.get_text(strip=True)

            # Only include forex/macro relevant news
            title_lower = title_text.lower()
            if not any(kw in title_lower for kw in FOREX_KEYWORDS):
                continue

            articles.append({
                "source": source_name,
                "title": title_text,
                "url": link_text,
                "pub_date": pub_date.get_text(strip=True) if pub_date else "",
            })

    except Exception as e:
        logger.error(f"RSS fetch error ({source_name}): {e}")

    return articles


async def fetch_all_news() -> list[dict]:
    """Fetch from all configured RSS feeds concurrently."""
    tasks = [fetch_rss(name, url) for name, url in RSS_FEEDS.items()]
    results = await asyncio.gather(*tasks)
    all_articles = []
    for result in results:
        all_articles.extend(result)
    return all_articles


# ═══════════════════════════════════════════
#  TWITTER / X  (optional — needs bearer token)
# ═══════════════════════════════════════════
async def fetch_twitter_news() -> list[dict]:
    """Fetch recent tweets from monitored accounts using Twitter v2 API."""
    if not TWITTER_BEARER:
        return []

    tweets = []
    headers = {"Authorization": f"Bearer {TWITTER_BEARER}"}

    # Search for keyword mentions in last 15 minutes
    keyword_query = " OR ".join(f'"{kw}"' for kw in TWITTER_KEYWORDS[:5])
    query = f"({keyword_query}) (forex OR FX OR USD OR EUR OR GBP OR JPY) lang:en -is:retweet"

    params = {
        "query": query,
        "max_results": 10,
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "username",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Twitter API {resp.status}")
                    return []
                data = await resp.json()

        tweet_data = data.get("data", [])
        users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}

        for tweet in tweet_data:
            tweet_id = tweet["id"]
            if tweet_id in seen_tweet_ids:
                continue
            username = users.get(tweet.get("author_id", ""), "unknown")
            tweets.append({
                "source": "Twitter/X",
                "title": f"@{username}: {tweet['text'][:200]}",
                "url": f"https://twitter.com/{username}/status/{tweet_id}",
                "tweet_id": tweet_id,
            })

    except Exception as e:
        logger.error(f"Twitter fetch error: {e}")

    return tweets


# ═══════════════════════════════════════════
#  MESSAGE FORMATTERS
# ═══════════════════════════════════════════
IMPACT_EMOJI = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
CURRENCY_PAIRS = {
    "USD": "EURUSD · USDJPY · GBPUSD · USDCAD",
    "EUR": "EURUSD · EURGBP · EURJPY · EURCHF",
    "GBP": "GBPUSD · EURGBP · GBPJPY",
    "JPY": "USDJPY · EURJPY · GBPJPY",
    "CAD": "USDCAD · CADJPY",
    "AUD": "AUDUSD · AUDNZD · AUDJPY",
    "NZD": "NZDUSD · AUDNZD",
    "CHF": "USDCHF · EURCHF · GBPCHF",
}


def format_pre_event(event: dict) -> str:
    imp = IMPACT_EMOJI.get(event["impact"], "⚪")
    pairs = CURRENCY_PAIRS.get(event["currency"], f"{event['currency']} pairs")
    return (
        f"⏰ *PRE-EVENT ALERT — {PRE_EVENT_MINUTES} MIN*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *{event['event']}*\n"
        f"💱 Currency: `{event['currency']}`\n"
        f"🕐 Time: `{event['time']} UTC`\n"
        f"📈 Forecast: `{event['forecast']}`\n"
        f"📊 Previous: `{event['previous']}`\n"
        f"{imp} Impact: *{event['impact']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍 Watch: {pairs}\n"
        f"#Forex #EconomicCalendar #{event['currency']}"
    )


def format_release(event: dict) -> str:
    imp = IMPACT_EMOJI.get(event["impact"], "⚪")
    actual = event.get("actual", "pending")
    
    # Simple beat/miss detection
    verdict = ""
    try:
        act_val = float(re.sub(r"[^\d.\-]", "", actual))
        fc_val = float(re.sub(r"[^\d.\-]", "", event["forecast"]))
        if act_val > fc_val:
            verdict = "📈 *Beat forecast* — potential bullish for " + event["currency"]
        elif act_val < fc_val:
            verdict = "📉 *Missed forecast* — potential bearish for " + event["currency"]
        else:
            verdict = "➡️ *Inline with forecast*"
    except (ValueError, TypeError):
        verdict = ""

    return (
        f"📊 *DATA RELEASED*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *{event['event']}*\n"
        f"💱 `{event['currency']}`\n"
        f"✅ Actual: `{actual}`\n"
        f"🎯 Forecast: `{event['forecast']}`\n"
        f"📊 Previous: `{event['previous']}`\n"
        f"{imp} Impact: *{event['impact']}*\n"
        f"{('━━━━━━━━━━━━━━━━━━━━' + chr(10) + verdict) if verdict else ''}\n"
        f"#Forex #{event['currency']} #Release"
    )


def format_breaking_news(article: dict) -> str:
    source_emoji = {
        "BBC News": "🇬🇧",
        "Al Jazeera": "🌍",
        "Fox Business": "🦅",
        "Reuters": "📡",
        "Bloomberg": "💹",
        "Twitter/X": "🐦",
    }.get(article["source"], "📰")

    return (
        f"🚨 *BREAKING — {article['source']}* {source_emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{article['title']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [Read more]({article['url']})\n"
        f"#BreakingNews #Forex #Markets"
    )


# ═══════════════════════════════════════════
#  BROADCAST HELPERS
# ═══════════════════════════════════════════
async def broadcast(bot: Bot, message: str):
    """Send message to channel and all individual subscribers."""
    targets = set()
    if CHANNEL_ID:
        targets.add(CHANNEL_ID)
    targets.update(subscribers)

    for chat_id in targets:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode="Markdown",
                disable_web_page_preview=False,
            )
            await asyncio.sleep(0.05)  # rate limit buffer
        except Exception as e:
            logger.error(f"Send error to {chat_id}: {e}")


# ═══════════════════════════════════════════
#  BACKGROUND JOBS
# ═══════════════════════════════════════════
async def job_check_calendar(context: ContextTypes.DEFAULT_TYPE):
    """Poll Forex Factory and send pre-event + release alerts."""
    bot: Bot = context.bot
    now = datetime.now(timezone.utc)
    events = await fetch_forex_factory()

    for event in events:
        event_time = parse_event_time(event["time"])
        if not event_time:
            continue

        # Pre-event alert
        minutes_to_event = (event_time - now).total_seconds() / 60
        if 0 < minutes_to_event <= PRE_EVENT_MINUTES and event["id"] not in alerted_events:
            alerted_events.add(event["id"])
            msg = format_pre_event(event)
            await broadcast(bot, msg)
            logger.info(f"Pre-event alert: {event['event']}")

        # Release alert — actual data appeared
        if event.get("actual") and event["id"] not in released_events:
            released_events.add(event["id"])
            msg = format_release(event)
            await broadcast(bot, msg)
            logger.info(f"Release alert: {event['event']} actual={event['actual']}")


async def job_check_news(context: ContextTypes.DEFAULT_TYPE):
    """Poll RSS feeds and Twitter for breaking news."""
    bot: Bot = context.bot

    # RSS feeds
    articles = await fetch_all_news()
    for article in articles:
        url = article["url"]
        if url and url not in seen_news_urls:
            seen_news_urls.add(url)
            msg = format_breaking_news(article)
            await broadcast(bot, msg)
            logger.info(f"Breaking news: {article['source']} — {article['title'][:60]}")
            await asyncio.sleep(1)  # brief pause between breaking news

    # Twitter/X (if token configured)
    tweets = await fetch_twitter_news()
    for tweet in tweets:
        tweet_id = tweet.get("tweet_id")
        if tweet_id:
            seen_tweet_ids.add(tweet_id)
        url = tweet["url"]
        if url not in seen_news_urls:
            seen_news_urls.add(url)
            msg = format_breaking_news(tweet)
            await broadcast(bot, msg)
            logger.info(f"Tweet alert: {tweet['title'][:60]}")


# ═══════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.add(chat_id)
    await update.message.reply_text(
        "👋 *Welcome to ForexPulse Bot!*\n\n"
        "You'll receive:\n"
        "⏰ Pre-event alerts (30 min before releases)\n"
        "📊 Actual data as soon as it drops\n"
        "🚨 Breaking news from BBC, Al Jazeera, Fox, Reuters\n"
        "🐦 Relevant tweets from key accounts\n\n"
        "Commands:\n"
        "/today — today's economic calendar\n"
        "/news — latest breaking news\n"
        "/stop — unsubscribe\n"
        "/help — show this message",
        parse_mode="Markdown",
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribers.discard(update.effective_chat.id)
    await update.message.reply_text("✅ You've been unsubscribed. Use /start to resubscribe anytime.")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's economic calendar."""
    await update.message.reply_text("⏳ Fetching calendar...")
    events = await fetch_forex_factory()

    if not events:
        await update.message.reply_text("No high-impact events found for today.")
        return

    lines = ["📅 *Today's Economic Calendar*\n"]
    for e in events:
        imp = IMPACT_EMOJI.get(e["impact"], "⚪")
        lines.append(f"{imp} `{e['time']}` — *{e['currency']}* {e['event']}")
        lines.append(f"   Forecast: `{e['forecast']}` | Prev: `{e['previous']}`\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show latest breaking news headlines."""
    await update.message.reply_text("⏳ Fetching latest news...")
    articles = await fetch_all_news()

    if not articles:
        await update.message.reply_text("No relevant news found right now.")
        return

    lines = ["🚨 *Latest Market News*\n"]
    for a in articles[:8]:
        lines.append(f"• [{a['title'][:80]}]({a['url']})\n  — _{a['source']}_\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)


# ═══════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # Register commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("help", cmd_help))

    # Schedule background jobs
    job_queue = app.job_queue
    job_queue.run_repeating(job_check_calendar, interval=CHECK_CALENDAR_EVERY, first=10)
    job_queue.run_repeating(job_check_news, interval=CHECK_NEWS_EVERY, first=30)

    logger.info("ForexPulse Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
