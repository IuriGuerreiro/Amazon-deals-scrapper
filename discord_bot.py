import asyncio
import json
import logging
import os
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord import HTTPException
from discord.errors import Forbidden, NotFound
from discord.ext import commands, tasks
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from main import AmazonDealsScraper


logger = logging.getLogger("amazon_deals.discord_bot")


DEFAULT_CONFIG = {
    "sites": [
        {
            "name": "Amazon Spain",
            "base_url": AmazonDealsScraper.DEFAULT_BASE_URL,
            "marketplace_id": AmazonDealsScraper.DEFAULT_MARKETPLACE_ID,
            "categories": [
                "Beauty",
                "Computer & Software",
                "Gaming & Accessories",
            ],
            "scrape_all": False,
        }
    ]
}


@dataclass
class DealChange:
    change_type: str
    changed_fields: Dict[str, Tuple[Optional[str], Optional[str]]]


def normalize_config(data: Dict) -> Dict:
    if not isinstance(data, dict):
        return deepcopy(DEFAULT_CONFIG)

    sites = data.get("sites")
    if not sites or not isinstance(sites, list):
        categories = data.get("categories", DEFAULT_CONFIG["sites"][0]["categories"])
        fallback_site = {
            "name": data.get("name", DEFAULT_CONFIG["sites"][0]["name"]),
            "base_url": data.get("base_url", AmazonDealsScraper.DEFAULT_BASE_URL),
            "marketplace_id": data.get(
                "marketplace_id", AmazonDealsScraper.DEFAULT_MARKETPLACE_ID
            ),
            "categories": categories,
            "scrape_all": data.get("scrape_all", False),
        }
        return {"sites": [fallback_site]}

    normalized_sites: List[Dict] = []
    for site in sites:
        if not isinstance(site, dict):
            continue

        normalized_sites.append(
            {
                "name": site.get("name") or "Amazon Site",
                "base_url": site.get("base_url", AmazonDealsScraper.DEFAULT_BASE_URL),
                "marketplace_id": site.get(
                    "marketplace_id", AmazonDealsScraper.DEFAULT_MARKETPLACE_ID
                ),
                "categories": site.get(
                    "categories", DEFAULT_CONFIG["sites"][0]["categories"]
                ),
                "scrape_all": site.get("scrape_all", False),
            }
        )

    if not normalized_sites:
        return deepcopy(DEFAULT_CONFIG)

    return {"sites": normalized_sites}


def load_config(path: str = "config.json") -> Dict:
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as config_file:
            json.dump(DEFAULT_CONFIG, config_file, indent=2, ensure_ascii=False)
        logger.info("Created default config.json")
        return deepcopy(DEFAULT_CONFIG)

    with open(path, "r", encoding="utf-8") as config_file:
        try:
            data = json.load(config_file)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid config.json, falling back to defaults: %s", exc)
            return deepcopy(DEFAULT_CONFIG)

    return normalize_config(data)


def ensure_env_vars() -> Tuple[str, int, str, str]:
    load_dotenv()

    token = os.getenv("DISCORD_TOKEN")
    channel_id_raw = os.getenv("DISCORD_CHANNEL_ID")
    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB", "amazon_deals")

    missing = [
        name
        for name, value in {
            "DISCORD_TOKEN": token,
            "DISCORD_CHANNEL_ID": channel_id_raw,
            "MONGODB_URI": mongo_uri,
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    try:
        channel_id = int(channel_id_raw)  # type: ignore[arg-type]
    except ValueError as exc:
        raise RuntimeError("DISCORD_CHANNEL_ID must be an integer") from exc

    return token, channel_id, mongo_uri, mongo_db


class DealMonitorBot(commands.Bot):
    def __init__(self, *, channel_id: int, mongo_uri: str, mongo_db: str):
        intents = discord.Intents.default()
        super().__init__(command_prefix=commands.when_mentioned_or("!"), intents=intents)

        self.channel_id = channel_id
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client[mongo_db]
        self.deals = self.db["deals"]

        self.deals_channel: Optional[discord.abc.Messageable] = None
        self.config = load_config()

    async def setup_hook(self) -> None:
        self.scrape_loop.start()
        self.loop.create_task(self.initial_sync())

    async def close(self) -> None:
        await super().close()
        self.mongo_client.close()

    @tasks.loop(hours=1)
    async def scrape_loop(self) -> None:
        await self.scrape_and_process(reason="Scheduled hourly sync")

    @scrape_loop.before_loop
    async def before_scrape_loop(self) -> None:
        await self.wait_until_ready()
        await self.ensure_channel()

    async def initial_sync(self) -> None:
        await self.wait_until_ready()
        await self.ensure_channel()
        await self.scrape_and_process(reason="Initial sync")

    async def ensure_channel(self) -> None:
        if self.deals_channel:
            return

        channel = self.get_channel(self.channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(self.channel_id)
            except NotFound as exc:
                raise RuntimeError(
                    f"Channel ID {self.channel_id} was not found. "
                    "Ensure the bot is invited to the server and the ID is correct."
                ) from exc
            except Forbidden as exc:
                raise RuntimeError(
                    f"Bot lacks access to channel ID {self.channel_id}. "
                    "Grant the bot permission to view and send messages in that channel."
                ) from exc

        if isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            self.deals_channel = channel
        else:
            raise RuntimeError(
                f"Channel ID {self.channel_id} is not a text-compatible channel"
            )

    async def scrape_and_process(self, *, reason: str) -> None:
        logger.info("Starting scrape cycle: %s", reason)

        sites = self.config.get("sites", [])
        if not sites:
            logger.warning("No sites configured for scraping")
            return

        collected: Dict[str, Dict] = {}

        for site_idx, site in enumerate(sites, start=1):
            site_name = site.get("name", "Amazon Site")
            base_url = site.get("base_url", AmazonDealsScraper.DEFAULT_BASE_URL)
            marketplace_id = site.get(
                "marketplace_id", AmazonDealsScraper.DEFAULT_MARKETPLACE_ID
            )
            categories = (
                AmazonDealsScraper.CATEGORIES
                if site.get("scrape_all")
                else site.get("categories", [])
            )

            if not categories:
                logger.warning("No categories configured for site '%s'", site_name)
                continue

            logger.info(
                "Site '%s' (%d/%d) scraping %d categories",
                site_name,
                site_idx,
                len(sites),
                len(categories),
            )

            for idx, category in enumerate(categories, start=1):
                scraper = AmazonDealsScraper(
                    category=category,
                    marketplace_id=marketplace_id,
                    base_url=base_url,
                    site_name=site_name,
                )
                try:
                    await scraper.scrape()
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.exception(
                        "Failed to scrape site '%s' category '%s': %s",
                        site_name,
                        category,
                        exc,
                    )
                    continue

                logger.info(
                    "Site '%s' category '%s' returned %d deals (%d/%d)",
                    site_name,
                    category,
                    len(scraper.deals),
                    idx,
                    len(categories),
                )

                for deal in scraper.deals:
                    asin = deal.get("asin")
                    if not asin or asin == "N/A":
                        continue

                    key = f"{marketplace_id}:{site_name}:{asin}"
                    stored = collected.get(key)
                    category_name = deal.get("category", "Unknown")

                    if stored:
                        categories_set = set(stored.get("categories", []))
                        categories_set.add(category_name)
                        stored["categories"] = sorted(categories_set)

                        # Prefer the latest non-N/A values for dynamic fields
                        for field, value in deal.items():
                            if field == "categories":
                                continue
                            if value and value != "N/A":
                                stored[field] = value
                    else:
                        deal_copy = dict(deal)
                        deal_copy["categories"] = [category_name]
                        deal_copy["marketplace_id"] = marketplace_id
                        deal_copy["site"] = site_name
                        deal_copy["base_url"] = base_url
                        collected[key] = deal_copy

                await asyncio.sleep(2)

        logger.info("Scrape cycle complete: %s", reason)

        if not collected:
            logger.info("No deals collected during scrape cycle")
            return

        logger.info("Processing %d unique deals after scraping", len(collected))

        notifications: List[Tuple[Dict, DealChange]] = []
        new_deal_counts = defaultdict(int)

        for deal in collected.values():
            try:
                change, is_new = await self.process_deal(deal)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.exception("Failed to process deal %s: %s", deal.get("asin"), exc)
                continue

            if is_new:
                site_name = deal.get("site", "Unknown Site")
                new_deal_counts[site_name] += 1

            if change:
                notifications.append((deal, change))

        if new_deal_counts:
            for site_name, count in new_deal_counts.items():
                logger.info("New deals detected for %s: %d", site_name, count)
        else:
            logger.info("No new deals detected across configured sites")

        if not notifications:
            logger.info("No deal changes detected after processing")
            return

        logger.info("Deals after processing: %d price updates queued", len(notifications))

        for deal, change in notifications:
            await self.notify_change(deal, change)

    async def process_deal(self, deal: Dict) -> Tuple[Optional[DealChange], bool]:
        marketplace_id = deal.get("marketplace_id")
        site_name = deal.get("site")

        query: Dict[str, str] = {"asin": deal["asin"]}
        if marketplace_id:
            query["marketplace_id"] = marketplace_id
        if site_name:
            query["site"] = site_name

        existing = await self.deals.find_one(query)

        now = datetime.now(timezone.utc)
        categories = set(existing.get("categories", [])) if existing else set()

        direct_category = deal.get("category")
        if direct_category:
            categories.add(direct_category)

        extra_categories = deal.get("categories", [])
        if isinstance(extra_categories, list):
            categories.update(extra_categories)

        document = {**deal, "categories": sorted(categories), "last_seen": now}

        if not existing:
            document["first_seen"] = now
            await self.deals.insert_one(document)
            logger.debug(
                "Inserted new deal %s (%s)", deal.get("asin"), deal.get("marketplace_id")
            )
            return DealChange("New deal", {}), True

        fields_to_watch: List[str] = ["current_price"]
        changed: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

        for field in fields_to_watch:
            old_value = existing.get(field)
            new_value = deal.get(field)
            if old_value != new_value:
                changed[field] = (old_value, new_value)

        update_doc = {
            "$set": document,
        }
        await self.deals.update_one({"_id": existing["_id"]}, update_doc)

        if changed:
            return DealChange("Updated deal", changed), False

        return None, False

    async def notify_change(self, deal: Dict, change: DealChange) -> None:
        if not self.deals_channel:
            logger.warning("Deals channel not ready; dropping notification")
            return

        embed = discord.Embed(
            title=deal.get("title", "Amazon Deal"),
            url=deal.get("product_url", ""),
            description=change.change_type,
            timestamp=datetime.now(timezone.utc),
            color=discord.Color.green() if change.change_type == "New deal" else discord.Color.orange(),
        )

        current_price = deal.get("current_price", "N/A")
        original_price = deal.get("original_price", "N/A")
        discount = deal.get("discount", "N/A")

        price_lines = [f"**Price:** {current_price}"]
        if original_price != "N/A":
            price_lines.append(f"**Original:** {original_price}")
        if discount != "N/A":
            price_lines.append(f"**Discount:** {discount}")

        embed.add_field(
            name="Pricing",
            value="\n".join(price_lines),
            inline=False,
        )

        badge = deal.get("deal_badge", "N/A")
        categories = ", ".join(deal.get("categories", []))

        embed.add_field(
            name="Details",
            value=f"**Badge:** {badge}\n**ASIN:** {deal.get('asin', 'N/A')}\n**Categories:** {categories}",
            inline=False,
        )

        if change.changed_fields:
            change_lines = []
            for field, (old_value, new_value) in change.changed_fields.items():
                change_lines.append(
                    f"• `{field}` changed from `{old_value or 'N/A'}` to `{new_value or 'N/A'}`"
                )
            embed.add_field(
                name="Changes",
                value="\n".join(change_lines),
                inline=False,
            )

        if deal.get("image_url") and deal["image_url"] != "N/A":
            embed.set_thumbnail(url=deal["image_url"])

        embed.set_footer(text="Amazon Deals Monitor")

        try:
            await self.deals_channel.send(embed=embed)
        except HTTPException as exc:
            if exc.status == 429:
                logger.warning("Hit Discord rate limit, waiting 5 seconds before retry")
                await asyncio.sleep(5)
                await self.deals_channel.send(embed=embed)
            else:
                raise

        await asyncio.sleep(0.5)


def run_bot() -> None:
    logging.basicConfig(level=logging.INFO)
    token, channel_id, mongo_uri, mongo_db = ensure_env_vars()
    bot = DealMonitorBot(channel_id=channel_id, mongo_uri=mongo_uri, mongo_db=mongo_db)
    bot.run(token)


if __name__ == "__main__":
    run_bot()
