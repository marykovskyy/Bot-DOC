import os
from dotenv import load_dotenv

load_dotenv(".env.example")

# --- СЕКРЕТИ (з .env, не з коду!) ---
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "790612931"))

if not TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN не знайдено в .env файлі!")

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
        "search_url": "https://bizfileonline.sos.ca.gov/search/business",
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
}
