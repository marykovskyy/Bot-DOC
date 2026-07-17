import os

from dotenv import load_dotenv

load_dotenv("token.env")

# --- СЕКРЕТИ (з .env, не з коду!) ---
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))

if not TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN не знайдено в .env файлі!")

# --- Google Maps Platform: Address Validation API ---
# Опційно. Якщо не задано — функція "перевірка адрес" вимикається graceful'но.
# Створення: https://console.cloud.google.com/ → проект → APIs & Services →
# Library → "Address Validation API" → Enable → Credentials → API key.
# Безкоштовно: 5 000 запитів/міс. Понад: $17 / 1 000 запитів.
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

# --- ПУБЛІЧНА КОНФІГУРАЦІЯ СКРАПЕРІВ ---
SCRAPER_CONFIG = {
    "France": {
        "flag": "🇫🇷",
        "search_url": "https://www.pappers.fr/recherche?q={kw}&etat=A&page={p}",
        "link_selector": 'a[href*="/entreprise/"]',
        "name_tag": "h1"
    },
    "Denmark": {
        "flag": "🇩🇰",
        "search_url": "https://datacvr.virk.dk/",
        "link_selector": "",
        "name_tag": ""
    },
    "Finland": {
        "flag": "🇫🇮",
        "search_url": "https://tietopalvelu.ytj.fi/?companyName={kw}&companyFormCodes=16&isCompanyValid=true&isCompanyTerminated=false",
        "link_selector": 'a[href*="/yritys/"]',
        "name_tag": "h1"
    },
    "California": {
        "flag": "🇺🇸",
        "group": "USA",
        "search_url": "https://bizfileonline.sos.ca.gov/search/business",
        "link_selector": "",
        "name_tag": ""
    },
    "Washington": {
        "flag": "🇺🇸",
        "group": "USA",
        "search_url": "https://ccfs.sos.wa.gov/#/AdvancedSearch",
        "link_selector": "",
        "name_tag": ""
    },
    "CzechRepublic": {
        "flag": "🇨🇿",
        "search_url": "https://or.justice.cz/ias/ui/rejstrik",
        "link_selector": "",
        "name_tag": ""
    },
    "UnitedKingdom": {
        "flag": "🇬🇧",
        "search_url": "API",
        "link_selector": "",
        "name_tag": ""
    },
    "Latvia": {
        "flag": "🇱🇻",
        "search_url": "API",
        "link_selector": "",
        "name_tag": ""
    },
    "Norway": {
        "flag": "🇳🇴",
        "search_url": "API",
        "link_selector": "",
        "name_tag": ""
    },
    "NewZealand": {
        "flag": "🇳🇿",
        "search_url": "DrissionPage",
        "link_selector": "",
        "name_tag": ""
    },
    "Thailand": {
        "flag": "🇹🇭",
        "search_url": "https://datawarehouse.dbd.go.th/index",
        "link_selector": "",
        "name_tag": ""
    },
    "Turkey": {
        "flag": "🇹🇷",
        "search_url": "https://bilgibankasi.ito.org.tr/tr/bilgi-bankasi/firma-bilgileri",
        "link_selector": "",
        "name_tag": ""
    },
    "India": {
        "flag": "🇮🇳",
        "search_url": "API",   # data.gov.in Open Data (MCA company master data)
        "link_selector": "",
        "name_tag": ""
    },
}
