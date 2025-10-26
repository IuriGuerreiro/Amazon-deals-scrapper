import asyncio
import json
import csv
import os
from urllib.parse import urlparse
from playwright.async_api import async_playwright

class AmazonDealsScraper:
    DEFAULT_MARKETPLACE_ID = "A1RKKUPIHCS9HS"
    DEFAULT_BASE_URL = "https://www.amazon.es/-/en/deals"

    # Category button names as they appear on the page
    CATEGORIES = [
        "Featured Deals",
        "Trending Deals",
        "Lightning Deals",
        "Deals under 20€",
        "Amazon Devices",
        "Prime Exclusive",
        "Computer & Software",
        "TV, Movies & Home Cinema",
        "Fashion, Shoes & Bags",
        "Home & Kitchen",
        "Phone & Accessories",
        "Personal Care & Grooming",
        "Headphones, Speakers & Music",
        "Sports & Fitness",
        "Gaming & Accessories",
        "Pet products",
        "Beauty",
        "DIY & Tools",
        "Toys",
        "Baby",
        "Office & School Supplies",
        "Cameras",
        "Food & Drinks",
        "Watches & Jewellery",
        "Furniture",
        "Luggage & Backpack",
        "Car & Motorbike",
        "Garden & Outdoors",
        "Books",
        "Vouchers",
        "Outlet",
    ]

    def __init__(self, marketplace_id=None, category=None, base_url=None, site_name=None):
        self.marketplace_id = marketplace_id or self.DEFAULT_MARKETPLACE_ID
        self.category = category
        self.base_url = base_url or self.DEFAULT_BASE_URL
        parsed = urlparse(self.base_url)
        host = (parsed.hostname or "www.amazon.es").lower()

        if host.startswith("data."):
            api_host = host
        elif host.startswith("www."):
            api_host = "data." + host[4:]
        else:
            api_host = "data." + host

        self.domain_host = host if not host.startswith("data.") else host.replace("data.", "www.", 1)
        self.site_name = site_name or self.domain_host
        self.api_url = f"https://{api_host}/api/marketplaces/{self.marketplace_id}/promotions"
        self.deals = []
        
        if self.category:
            print(f"🎯 [{self.site_name}] Category: {self.category}")
        else:
            print(f"🎯 [{self.site_name}] Category: ALL (No filter)")

    def parse_promotion(self, promo):
        """Extract deal information from promotion object"""
        try:
            product = promo.get("product", {}).get("entity", {})
            asin = product.get("asin", "N/A")
            
            # Title - extract displayString from title entity
            title = "N/A"
            title_data = product.get("title", {})
            if isinstance(title_data, dict):
                entity = title_data.get("entity", {})
                if isinstance(entity, dict):
                    title = entity.get("displayString", "N/A")
            
            # Pricing
            buying_options = product.get("buyingOptions", [])
            current_price = "N/A"
            original_price = "N/A"
            discount = "N/A"
            
            if buying_options:
                price_info = buying_options[0].get("price", {}).get("entity", {})
                
                price_to_pay = price_info.get("priceToPay", {})
                if isinstance(price_to_pay, dict):
                    amount = price_to_pay.get("moneyValueOrRange", {}).get("value", {}).get("amount")
                    current_price = f"€{amount}" if amount else "N/A"
                
                basis_price = price_info.get("basisPrice", {})
                if isinstance(basis_price, dict):
                    amount = basis_price.get("moneyValueOrRange", {}).get("value", {}).get("amount")
                    original_price = f"€{amount}" if amount else "N/A"
                
                savings = price_info.get("savings", {})
                if isinstance(savings, dict):
                    value = savings.get("percentage", {}).get("value")
                    discount = f"{value}%" if value else "N/A"
            
            # Deal badge
            deal_label = "N/A"
            if buying_options:
                deal_badge = buying_options[0].get("dealBadge", {})
                if isinstance(deal_badge, dict):
                    label = deal_badge.get("entity", {}).get("label", {})
                    if isinstance(label, dict):
                        fragments = label.get("content", {}).get("fragments", [])
                        deal_label = fragments[0].get("text", "N/A") if fragments else "N/A"
            
            # Image
            image_url = "N/A"
            images = product.get("productImages", {}).get("entity", {}).get("images", [])
            if images:
                image_id = images[0].get("lowRes", {}).get("physicalId")
                image_url = f"https://m.media-amazon.com/images/I/{image_id}._AC_SF226,226_QL85_.jpg" if image_id else "N/A"
            
            # Product URL
            product_host = self.domain_host if self.domain_host else "www.amazon.es"
            if not product_host.startswith("http"):
                product_url_base = f"https://{product_host}"
            else:
                product_url_base = product_host
            product_url = f"{product_url_base}/dp/{asin}" if asin != "N/A" else "N/A"
            
            # Brand ID
            brand_id = promo.get("brandId", "N/A")
            
            return {
                "title": title,
                "asin": asin,
                "product_url": product_url,
                "discount": discount,
                "current_price": current_price,
                "original_price": original_price,
                "deal_badge": deal_label,
                "category": self.category if self.category else "All Categories",
                "brand_id": brand_id,
                "image_url": image_url,
                "marketplace_id": self.marketplace_id,
                "site": self.site_name,
                "base_url": self.base_url,
            }
        except Exception as e:
            return None

    async def intercept_api_calls(self, page):
        """Intercept and capture API responses"""
        async def handle_response(response):
            if self.api_url in response.url and response.status == 200:
                try:
                    data = await response.json()
                    if "entity" in data and "rankedPromotions" in data["entity"]:
                        promotions = data["entity"]["rankedPromotions"]
                        print(f"   ✓ Intercepted {len(promotions)} deals from page")
                        
                        for promo in promotions:
                            deal = self.parse_promotion(promo)
                            if deal:
                                self.deals.append(deal)
                except:
                    pass
        
        page.on("response", handle_response)

    async def scrape(self, max_pages=None):
        """Scrape Amazon deals using Playwright - continues until no new products found"""
        async with async_playwright() as p:
            print("🚀 Starting browser...")
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Set up response interception
            await self.intercept_api_calls(page)
            
            print(f"📄 Loading Amazon deals page: {self.base_url}")
            await page.goto(self.base_url, wait_until="networkidle", timeout=30000)
            
            # Click category button if specified
            if self.category:
                print(f"   🔘 Clicking category: {self.category}")
                try:
                    button = page.locator(f'button:has-text("{self.category}")').first
                    await button.click(timeout=5000)
                    await page.wait_for_timeout(3000)
                    print(f"   ✓ Category selected")
                except Exception as e:
                    print(f"   ⚠️  Could not click category button: {e}")
            
            print("⏳ Waiting for API calls...")
            await page.wait_for_timeout(5000)
            
            # Scroll until no new products are found
            page_num = 1
            previous_count = len(self.deals)
            no_new_products_count = 0
            max_no_new_attempts = 3
            
            while True:
                print(f"\n📦 Loading more deals (page {page_num + 1})...")
                
                # Try to find and click "View more deals" or "Show more" button
                show_more_selectors = [
                    '[data-testid="load-more-view-more-button"]',
                    'button[data-testid="load-more-view-more-button"]',
                    'button:has-text("View more deals")',
                    'button:has-text("Show more")',
                    'button[aria-label*="Show more"]',
                    'a[aria-label*="Show more"]',
                ]
                
                button_clicked = False
                for selector in show_more_selectors:
                    try:
                        button = page.locator(selector).first
                        if await button.is_visible(timeout=2000):
                            print(f"   🔘 Found 'Show more' button, clicking...")
                            await button.click()
                            await page.wait_for_timeout(3000)
                            button_clicked = True
                            break
                    except:
                        continue
                
                # If no button found, scroll instead
                if not button_clicked:
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                    await page.wait_for_timeout(3000)
                
                current_count = len(self.deals)
                new_products = current_count - previous_count
                
                if new_products == 0:
                    no_new_products_count += 1
                    print(f"   ⚠️  No new products found ({no_new_products_count}/{max_no_new_attempts})")
                    
                    if no_new_products_count >= max_no_new_attempts:
                        print(f"   ✓ Reached end of available deals")
                        break
                else:
                    print(f"   ✓ Found {new_products} new products (Total: {current_count})")
                    no_new_products_count = 0  # Reset counter when new products are found
                
                previous_count = current_count
                page_num += 1
                
                if max_pages and page_num > max_pages:
                    print(f"   ✓ Reached maximum pages limit ({max_pages})")
                    break
            
            await browser.close()
            return self.deals

    def print_deals(self, limit=10):
        """Print deals in readable format"""
        category_display = self.category if self.category else "All Categories"
        print(f"\n{'='*80}")
        print(f"✅ RESULTS: Found {len(self.deals)} deals in {category_display}")
        print(f"{'='*80}\n")
        
        if not self.deals:
            return
        
        for i, deal in enumerate(self.deals[:limit], 1):
            print(f"{i}. {deal['title'][:70]}")
            print(f"   ASIN: {deal['asin']} | Category: {deal['category']}")
            print(f"   💰 {deal['current_price']} (was {deal['original_price']}) | {deal['discount']} off")
            print(f"   🏷️  {deal['deal_badge']}")
            print(f"   🔗 {deal['product_url']}")
            print(f"   🌍 {deal.get('site', self.site_name)} ({deal.get('marketplace_id', self.marketplace_id)})")
            print()

    def save_to_json(self, filename=None):
        """Save to JSON"""
        if filename is None:
            category_name = self.category.lower().replace(" ", "_").replace("&", "and") if self.category else "all"
            filename = f"amazon_deals_{category_name}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.deals, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(self.deals)} deals to {filename}")

    def save_to_csv(self, filename=None):
        """Save to CSV"""
        if filename is None:
            category_name = self.category.lower().replace(" ", "_").replace("&", "and") if self.category else "all"
            filename = f"amazon_deals_{category_name}.csv"
        if not self.deals:
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.deals[0].keys())
            writer.writeheader()
            writer.writerows(self.deals)
        print(f"✓ Saved {len(self.deals)} deals to {filename}")

    def search_deals(self, keyword):
        """Search deals by keyword (case-insensitive)"""
        keyword_lower = keyword.lower()
        results = [deal for deal in self.deals if keyword_lower in deal['title'].lower()]
        return results

    def filter_by_discount(self, min_discount=10):
        """Filter deals by minimum discount percentage"""
        results = []
        for deal in self.deals:
            try:
                discount_val = int(deal['discount'].replace('%', ''))
                if discount_val >= min_discount:
                    results.append(deal)
            except:
                pass
        return results

    def print_search_results(self, results, title="Search Results"):
        """Print search results"""
        print(f"\n{'='*80}")
        print(f"🔍 {title}: {len(results)} items found")
        print(f"{'='*80}\n")
        
        for i, deal in enumerate(results, 1):
            print(f"{i}. {deal['title'][:70]}")
            print(f"   ASIN: {deal['asin']} | {deal['discount']} off")
            print(f"   💰 {deal['current_price']} (was {deal['original_price']})")
            print(f"   🔗 {deal['product_url']}")
            print()


async def main():
    print("\n" + "="*80)
    print("🛒 Amazon Spain Deals Scraper (Dynamic Category Filter)")
    print("="*80 + "\n")
    
    # Load config
    config_file = "config.json"
    
    # Create default config if it doesn't exist
    if not os.path.exists(config_file):
        default_config = {
            "sites": [
                {
                    "name": "Amazon Spain",
                    "base_url": AmazonDealsScraper.DEFAULT_BASE_URL,
                    "marketplace_id": AmazonDealsScraper.DEFAULT_MARKETPLACE_ID,
                    "categories": [
                        "Beauty",
                        "Computer & Software",
                        "Gaming & Accessories"
                    ],
                    "scrape_all": False
                }
            ]
        }
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        print(f"✓ Created {config_file} with default sites and categories")
        print("  Edit it to customize which sites to scrape\n")
        config = default_config
    else:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✓ Loaded {config_file}\n")
    
    sites = config.get("sites")
    if not sites:
        legacy_categories = config.get("categories", [
            "Beauty",
            "Computer & Software",
            "Gaming & Accessories"
        ])
        sites = [
            {
                "name": "Amazon Spain",
                "base_url": AmazonDealsScraper.DEFAULT_BASE_URL,
                "marketplace_id": AmazonDealsScraper.DEFAULT_MARKETPLACE_ID,
                "categories": legacy_categories,
                "scrape_all": config.get("scrape_all", False)
            }
        ]
    
    try:
        all_deals = []
        
        for site_index, site in enumerate(sites, 1):
            site_name = site.get("name", "Amazon Site")
            base_url = site.get("base_url", AmazonDealsScraper.DEFAULT_BASE_URL)
            marketplace_id = site.get("marketplace_id", AmazonDealsScraper.DEFAULT_MARKETPLACE_ID)
            scrape_all = site.get("scrape_all", False)
            categories_to_scrape = site.get("categories", [])
            
            if scrape_all:
                categories_to_scrape = AmazonDealsScraper.CATEGORIES
            
            if not categories_to_scrape:
                print(f"⚠️  No categories configured for {site_name}, skipping")
                continue
            
            print(f"\n{'='*80}")
            print(f"🌍 [{site_index}/{len(sites)}] Scraping site: {site_name}")
            print(f"{'='*80}")
            
            for i, category in enumerate(categories_to_scrape, 1):
                print(f"\n{'-'*80}")
                print(f"📂 [{i}/{len(categories_to_scrape)}] Category: {category}")
                print(f"{'-'*80}")
                
                scraper = AmazonDealsScraper(
                    marketplace_id=marketplace_id,
                    category=category,
                    base_url=base_url,
                    site_name=site_name,
                )
                await scraper.scrape()  # Scrapes until no new products found
                scraper.print_deals(limit=10)
                
                if scraper.deals:
                    all_deals.extend(scraper.deals)
                    print(f"✅ Scraped {len(scraper.deals)} deals from {category} at {site_name}")
                else:
                    print(f"⚠️  No deals found for {category} at {site_name}")
                
                if i < len(categories_to_scrape):
                    print(f"⏳ Waiting 3 seconds before next category...")
                    await asyncio.sleep(3)
            
            if site_index < len(sites):
                print(f"\n⏳ Waiting 5 seconds before next site...")
                await asyncio.sleep(5)
        
        # Save combined results
        if all_deals:
            print(f"\n{'='*80}")
            print(f"✅ FINAL RESULTS")
            print(f"{'='*80}")
            print(f"Total deals scraped: {len(all_deals)}")
            print("Data kept in memory for further processing.\n")
        else:
            print("⚠️  No deals scraped from any configured site.")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    # Install: pip install playwright
    # Setup: playwright install chromium
    asyncio.run(main())
