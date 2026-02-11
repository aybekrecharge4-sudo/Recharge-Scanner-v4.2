"""
RECHARGE.COM OPPORTUNITY SCANNER v4.2
16 data sources | Google Search grounding | Composite scoring | 4-pass AI
Single-file script -> HTML dashboard + Word + Email + GitHub Pages
Works in: Google Colab, GitHub Actions, local Python
"""

# =============================================================================
# SECTION 0 - IMPORTS & CONFIG
# =============================================================================

import os, sys, json, re, time, logging, subprocess, html as _html
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from urllib.parse import quote, urlparse
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("v4")

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")
DASH_PASSWORD = os.environ.get("DASH_PASSWORD", "")  # if set, dashboard requires password

NOW  = datetime.now()
YEAR = NOW.year
MON  = NOW.strftime("%B")
DATE = NOW.strftime("%Y-%m-%d")
TIME = NOW.strftime("%H:%M")

IS_COLAB = False
try:
    import google.colab
    IS_COLAB = True
except ImportError: pass

W = {
    "trends": .11, "reddit": .09, "steam": .09, "news": .12,
    "youtube": .08, "wiki": .02, "competitor": .06, "cheapshark": .06,
    "steamspy": .05, "gamerpower": .04, "epic": .05, "steam_new": .04,
    "gog": .05, "humble": .05, "freetogame": .04, "anime": .05,
}

CONF = {1: 0.55, 2: 0.75, 3: 0.90, 4: 1.0}
CONF_DEFAULT = 1.15
FUZZ_T = 0.72
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

KW = {
    "EA Sports FC":  ["EA FC","FC 26","FC 25","FIFA","FUT","TOTY","TOTS",
                      "Ultimate Team","FIFA Points","FC Points","Madden"],
    "PlayStation":   ["PlayStation","PS5","PS4","PS Plus","PlayStation Plus",
                      "PSN","PlayStation Store","PS VR","PlayStation Portal","DualSense"],
    "Xbox":          ["Xbox","Game Pass","Xbox Live","Xbox Series",
                      "Game Pass Ultimate","Game Pass Core","Xbox Cloud"],
    "Nintendo":      ["Nintendo","Switch","eShop","Nintendo Direct",
                      "Nintendo Online","Pokemon","Zelda","Mario","Switch 2","Animal Crossing"],
    "Steam":         ["Steam","Steam Deck","Steam Sale","Steam Wallet",
                      "Valve","Steam Fest","Steam Next Fest","Steam Summer","Steam Winter","Steam Spring"],
    "Fortnite":      ["Fortnite","V-Bucks","Battle Pass","Epic Games"],
    "Call of Duty":  ["Call of Duty","COD","Warzone","Modern Warfare","Black Ops","COD Points"],
    "GTA":           ["GTA","Grand Theft Auto","GTA 6","GTA Online","Shark Card","Rockstar"],
    "Minecraft":     ["Minecraft","Minecoins","Minecraft Realms"],
    "Roblox":        ["Roblox","Robux"],
    "Valorant":      ["Valorant","Valorant Points"],
    "League of Legends": ["League of Legends","LoL","Riot Points","Riot Games","Arcane"],
    "Genshin Impact":["Genshin","Primogems","Genesis Crystals","HoYoverse","Traveler","Teyvat"],
    "Honkai":        ["Honkai Star Rail","Honkai Impact","Penacony"],
    "PUBG Mobile":   ["PUBG Mobile","PUBG UC","Royale Pass","PUBG"],
    "Free Fire":     ["Free Fire","Free Fire Diamonds"],
    "Mobile Legends":["Mobile Legends","MLBB"],
    "Spotify":       ["Spotify","Spotify Premium","Spotify Wrapped"],
    "Netflix":       ["Netflix","Netflix Games","Squid Game"],
    "Disney Plus":   ["Disney Plus","Disney+","Hotstar","Star Wars","Marvel","Mandalorian"],
    "Amazon Prime":  ["Prime Video","Prime Gaming","Amazon Prime","Twitch Prime"],
    "YouTube Premium":["YouTube Premium","YouTube Music"],
    "Apple":         ["Apple TV","Apple Music","iTunes","App Store","Apple gift card","Apple Arcade"],
    "Google Play":   ["Google Play","Play Store","Google gift card"],
    "Discord":       ["Discord","Nitro","Discord Nitro"],
    "Twitch":        ["Twitch","Twitch bits","Twitch sub"],
    "Gift Cards":    ["gift card","prepaid","voucher","e-gift","top-up","recharge","digital code"],
    "Paysafecard":   ["paysafecard","Neosurf"],
    "Razer Gold":    ["Razer Gold","Karma Koin"],
    "Crunchyroll":   ["Crunchyroll","anime streaming","Funimation","One Piece","Dragon Ball",
                      "Demon Slayer","Jujutsu Kaisen","My Hero Academia","Naruto","Bleach","anime"],
    "Meta Quest":    ["Meta Quest","Quest 3","Oculus","VR","Quest Pro"],
    "Overwatch":     ["Overwatch","Overwatch 2"],
    "World of Warcraft": ["World of Warcraft","WoW","WoW Token","Blizzard","Diablo","Hearthstone"],
    "Counter-Strike":["CS2","Counter-Strike","CS:GO"],
    "Apex Legends":  ["Apex Legends","Apex Coins"],
    "Dead by Daylight": ["Dead by Daylight","DBD","Auric Cells"],
    "Esports":       ["esports","tournament","championship","Worlds","Major"],
    "Destiny":       ["Destiny 2","Bungie","Silver"],
    "Elden Ring":    ["Elden Ring","FromSoftware","Dark Souls"],
    "Monster Hunter":["Monster Hunter","Capcom","MH Wilds"],
}

RSS_FEEDS = {
    "IGN":"https://feeds.feedburner.com/ign/all",
    "GameSpot":"https://www.gamespot.com/feeds/mashup/",
    "Kotaku":"https://kotaku.com/rss",
    "PC Gamer":"https://www.pcgamer.com/rss/",
    "Eurogamer":"https://www.eurogamer.net/feed",
    "The Verge":"https://www.theverge.com/rss/index.xml",
    "Polygon":"https://www.polygon.com/rss/index.xml",
    "GamesRadar":"https://www.gamesradar.com/rss/",
    "Dexerto":"https://www.dexerto.com/feed/",
    "Destructoid":"https://www.destructoid.com/feed/",
    "VG247":"https://www.vg247.com/feed/all",
    "RPS":"https://www.rockpapershotgun.com/feed",
    "Push Square":"https://www.pushsquare.com/feeds/latest",
    "Nintendo Life":"https://www.nintendolife.com/feeds/latest",
    "Pure Xbox":"https://www.purexbox.com/feeds/latest",
    "Siliconera":"https://www.siliconera.com/feed/",
    "TouchArcade":"https://toucharcade.com/feed/",
    "XboxWire":"https://news.xbox.com/en-us/feed/",
    "PlayStation Blog":"https://blog.playstation.com/feed/",
}

YT_CHANNELS = {
    "IGN":"UCKy1dAqELo0zrOtPkf0eTMw","GameSpot":"UCbu2SsF1frCRhGHstdXZR5g",
    "PlayStation":"UC-2Y8dQb0S6DtpxNgAKoJKA","Xbox":"UCXGgrKt94gR6lmN4aN3mYTg",
    "Nintendo":"UCGIY_O-8vW4rfx98KlMkvRg","TheGameAwards":"UCMjezPLBl-5fVS0HERR6QRA",
    "SkillUp":"UCZ7AeeVbyslLM_8-nQy_8CQ","ACG":"UCK9_x1DImhU-eolIay5rb2Q",
    "DigitalFoundry":"UC9PBzalIcEQCsiIkq36PyUA","Laymen Gaming":"UCYkgPmEwIcTn_WmocdZXEcg",
    "Fextralife":"UC1ONOluGA4Hht2CsMkJOyVg",
}

SUBREDDITS = [
    "gaming","Games","pcgaming","PS5","XboxSeriesX","NintendoSwitch",
    "Steam","FortNiteBR","EASportsFC","GenshinImpact","leagueoflegends",
    "VALORANT","Roblox","spotify","netflix","GameDeals",
    "FreeGameFindings","anime","CrunchyrollPremium","NintendoSwitch2",
]

COMPETITORS = {"G2A":"https://www.g2a.com/","Eneba":"https://www.eneba.com/",
               "CDKeys":"https://www.cdkeys.com/","Kinguin":"https://www.kinguin.net/"}

WIKI_PAGES = [
    "Grand_Theft_Auto_VI","EA_Sports_FC","Fortnite","PlayStation_5",
    "Xbox_Game_Pass","Nintendo_Switch_2","Genshin_Impact","Call_of_Duty",
    "Minecraft","Roblox","Valorant","League_of_Legends","Steam_(service)",
    "Spotify","Netflix","Crunchyroll","Discord","Honkai:_Star_Rail",
    "Elden_Ring","Monster_Hunter_Wilds",
]

TREND_BATCHES = [
    ["EA FC 26","Fortnite","GTA 6","PS Plus","Game Pass"],
    ["Steam sale","Nintendo Switch 2","Roblox","Genshin Impact","Valorant"],
    ["Spotify Premium","Netflix","Discord Nitro","gift card","V-Bucks"],
    ["Call of Duty","Minecraft","Xbox","PlayStation","Steam Deck"],
    ["Crunchyroll","anime 2026","Monster Hunter Wilds","Elden Ring","Destiny 2"],
]

# Auto-generate news topics from every KW category + extras
_AUTO_TOPICS = []
for _cat, _kws in KW.items():
    _AUTO_TOPICS.append(f"{_cat} update")
    _AUTO_TOPICS.append(f"{_cat} news")
    if _kws: _AUTO_TOPICS.append(f"{_kws[0]} event")

NEWS_TOPICS = list(dict.fromkeys(_AUTO_TOPICS + [
    "EA FC 26 TOTY","EA FC 26 promo","EA FC 26 Future Stars",
    f"PlayStation Plus {MON} {YEAR}","PS Plus free games",
    "PlayStation State of Play","PS5 Pro",
    f"Xbox Game Pass {MON} {YEAR}","Xbox Developer Direct",
    "Nintendo Direct","Nintendo Switch 2",
    "Fortnite new season","GTA 6","GTA 6 release date",
    "Call of Duty new season","Warzone new season",
    "Genshin Impact new banner","Honkai Star Rail update",
    "Overwatch 2 event","Overwatch 2 season",
    "Diablo 4 season","Hearthstone expansion",
    "gaming gift card deals","game release date",
    "esports tournament","Epic Games free",
    "anime new season","One Piece","Dragon Ball",
    "Monster Hunter Wilds","Elden Ring DLC",
    "GOG sale","Humble Bundle","free game giveaway",
    "new game release this week","gaming news today",
    "biggest game releases this month","trending games",
    "game awards","gaming event this week",
]))

# =============================================================================
# SECTION 1 - DEPENDENCIES
# =============================================================================

def _install():
    pkgs = ["google-genai","pytrends","python-docx","gspread","google-auth",
            "requests","beautifulsoup4","feedparser"]
    for p in pkgs:
        mod = p.replace("-","_").split("[")[0]
        if mod == "python_docx": mod = "docx"
        if mod == "google_genai": mod = "google.genai"
        try: __import__(mod)
        except ImportError:
            subprocess.check_call([sys.executable,"-m","pip","install",p,"-q"])

print("Installing deps...", end=" "); _install(); print("OK")

import requests
from bs4 import BeautifulSoup
import feedparser
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

from google import genai
from google.genai import types as gtypes

GCLIENT = genai.Client(api_key=GEMINI_KEY)
GEMINI_MODEL = "gemini-2.5-flash"

try:
    import gspread; from google.auth import default as _gauth
    HAS_GSPREAD = True
except ImportError: HAS_GSPREAD = False

try:
    from pytrends.request import TrendReq
    HAS_PYTRENDS = True
except ImportError: HAS_PYTRENDS = False

# =============================================================================
# SECTION 2 - DATA MODELS
# =============================================================================

@dataclass
class Signal:
    source: str; title: str; desc: str = ""; url: str = ""
    score: float = 0.0; meta: dict = field(default_factory=dict)

@dataclass
class Candidate:
    title: str; signals: List[Signal] = field(default_factory=list)
    sources: int = 0; score: float = 0.0; url: str = ""
    category: str = ""; categories: List[str] = field(default_factory=list)

# =============================================================================
# SECTION 3 - UTILITIES
# =============================================================================

def GET(url, headers=None, timeout=15, retries=2):
    h = headers or {"User-Agent": UA}
    for i in range(retries+1):
        try:
            r = requests.get(url, headers=h, timeout=timeout)
            if r.status_code == 200: return r
            if r.status_code == 429: time.sleep(1.5*(2**i)); continue
            return None
        except:
            if i < retries: time.sleep(1*(i+1))
    return None

def fuzz(a, b, t=FUZZ_T):
    a2 = re.sub(r'[^a-z0-9 ]','',a.lower())
    b2 = re.sub(r'[^a-z0-9 ]','',b.lower())
    return SequenceMatcher(None,a2,b2).ratio() >= t if a2 and b2 else False

def cats(text):
    lo = text.lower()
    return [c for c,kws in KW.items() if any(k.lower() in lo for k in kws)]

def recent(ds, days=7):
    if not ds: return False
    cut = NOW - timedelta(days=days)
    for f in ['%a, %d %b %Y %H:%M:%S %z','%a, %d %b %Y %H:%M:%S %Z',
              '%Y-%m-%dT%H:%M:%S%z','%Y-%m-%dT%H:%M:%SZ',
              '%Y-%m-%dT%H:%M:%S.%f%z','%Y-%m-%d %H:%M:%S','%Y-%m-%d']:
        try:
            p = datetime.strptime(ds.replace('GMT','+0000'),f)
            if p.tzinfo: p = p.replace(tzinfo=None)
            return p >= cut
        except: continue
    return False

def mass_appeal(t):
    lo = t.lower()
    return not any(x in lo for x in
        ["my ","i ","i'm","i've","me ","just got","finally",
         "my friend","helped me","unpopular opinion","does anyone",
         "question about","need help","eli5","ama"])

def norm(v, mx): return min(100.0, v/mx*100.0) if mx > 0 else 0.0
def esc(s): return _html.escape(str(s))

def _best_url(signals):
    prio = {"news":1,"reddit":2,"youtube":3,"steam":4,"gog":5,"humble":6,"cheapshark":7,
            "epic":8,"anime":9,"gamerpower":10,"steam_new":11,"steamspy":12,
            "freetogame":13,"competitor":14,"trends":15,"wiki":99}
    urls = [(s.url, prio.get(s.source,20)) for s in signals if s.url]
    if not urls: return ""
    urls.sort(key=lambda x: x[1])
    return urls[0][0]

def _fmt_action(a):
    if isinstance(a, dict):
        owner = a.get("owner","")
        action = a.get("action","")
        due = a.get("due","")
        parts = []
        if owner: parts.append(f"<strong>{esc(owner)}</strong>: ")
        parts.append(esc(action))
        if due: parts.append(f" <em>({esc(due)})</em>")
        return "".join(parts)
    return esc(str(a))

def _match_cand(title, cands):
    """Find BEST matching candidate (not first-above-threshold). Prevents wrong URL matches."""
    best = None; best_r = 0
    a = re.sub(r'[^a-z0-9 ]','',title.lower())
    if not a: return None
    for c in cands:
        b = re.sub(r'[^a-z0-9 ]','',c.title.lower())
        if not b: continue
        r = SequenceMatcher(None,a,b).ratio()
        if r > best_r: best_r = r; best = c
    return best if best_r > 0.45 else None

def _domain(url):
    if not url: return ""
    try: return urlparse(url).netloc.replace("www.","")[:30]
    except: return ""

# =============================================================================
# SECTION 4 - 16 DATA FETCHERS
# =============================================================================

class TrendsFetcher:
    def fetch(self):
        if not HAS_PYTRENDS: return []
        out = []
        try:
            pt = TrendReq(hl='en-US',tz=360,retries=2,backoff_factor=0.5,
                          requests_args={'headers':{'Cookie':'CONSENT=YES+'}})
            try:
                tr = pt.trending_searches(pn='united_states')
                for _,row in tr.head(20).iterrows():
                    q = str(row[0]); c = cats(q)
                    if c: out.append(Signal("trends",q,"Trending search (US)",score=80,meta={"cats":c}))
            except: pass
            for batch in TREND_BATCHES:
                try:
                    pt.build_payload(batch,cat=0,timeframe='now 7-d',geo='',gprop='')
                    df = pt.interest_over_time()
                    if df.empty: continue
                    for kw in batch:
                        if kw not in df.columns: continue
                        vals = df[kw].tolist(); cur = float(vals[-1]) if vals else 0
                        out.append(Signal("trends",kw,f"Search interest: {cur:.0f}/100",
                            score=cur,meta={"cats":cats(kw)}))
                    time.sleep(2)
                except: time.sleep(3)
        except: pass
        log.info(f"Trends: {len(out)}"); return out

class RedditFetcher:
    def fetch(self):
        out = []
        for sub in SUBREDDITS:
            try:
                feed = feedparser.parse(f"https://www.reddit.com/r/{sub}/hot/.rss?limit=15",
                    request_headers={"User-Agent":"RechargeScanner/4.2"})
                for e in feed.entries[:12]:
                    t = e.get("title","")
                    if not mass_appeal(t): continue
                    cc = cats(t)
                    if not cc: continue
                    out.append(Signal("reddit",t[:150],f"r/{sub} (Hot)",
                        url=e.get("link",""),score=65,meta={"sub":sub,"cats":cc}))
                time.sleep(2)
            except: continue
        log.info(f"Reddit: {len(out)}"); return out

class SteamFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://store.steampowered.com/api/featured/")
            if r:
                for it in r.json().get("featured_win",[])[:10]:
                    n,d,aid = it.get("name",""),it.get("discount_percent",0),it.get("id","")
                    out.append(Signal("steam",n,f"Featured{f', {d}% off' if d else ''}",
                        url=f"https://store.steampowered.com/app/{aid}",
                        score=min(60+d*0.4,100),meta={"discount":d,"cats":cats(n)}))
        except: pass
        try:
            r = GET("https://store.steampowered.com/api/featuredcategories/")
            if r:
                data = r.json()
                for sec in ["top_sellers","specials"]:
                    for it in data.get(sec,{}).get("items",[])[:10]:
                        n,d,aid = it.get("name",""),it.get("discount_percent",0),it.get("id","")
                        out.append(Signal("steam",n,f"{sec.replace('_',' ').title()}{f', {d}% off' if d else ''}",
                            url=f"https://store.steampowered.com/app/{aid}",
                            score=min((70 if sec=="top_sellers" else 50)+d*0.3,100),
                            meta={"discount":d,"cats":cats(n)}))
        except: pass
        log.info(f"Steam: {len(out)}"); return out

class WikiFetcher:
    def fetch(self):
        out = []
        end = NOW-timedelta(days=1); s1 = end-timedelta(days=6)
        fmt = lambda d: d.strftime("%Y%m%d")
        hd = {"User-Agent":"RechargeScanner/4.2 (content-research)"}
        for pg in WIKI_PAGES:
            try:
                r = GET(f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{pg}/daily/{fmt(s1)}/{fmt(end)}",headers=hd,timeout=10)
                if r:
                    views = sum(d.get("views",0) for d in r.json().get("items",[]))
                    name = pg.replace("_"," ")
                    if views > 1000:
                        out.append(Signal("wiki",name,f"{views:,} views this week",
                            url=f"https://en.wikipedia.org/wiki/{pg}",
                            score=norm(views,200000),meta={"views":views,"cats":cats(name)}))
            except: continue
        log.info(f"Wiki: {len(out)}"); return out

class YTFetcher:
    def fetch(self):
        out = []; ns = {"a":"http://www.w3.org/2005/Atom"}
        for ch,cid in YT_CHANNELS.items():
            try:
                r = GET(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}",timeout=10)
                if not r: continue
                root = ET.fromstring(r.content)
                for e in root.findall("a:entry",ns)[:10]:
                    te = e.find("a:title",ns); le = e.find("a:link",ns); pe = e.find("a:published",ns)
                    if te is None: continue
                    t = te.text or ""; u = le.get("href","") if le is not None else ""
                    pub = pe.text if pe is not None else ""
                    if not recent(pub,7): continue
                    cc = cats(t)
                    if cc: out.append(Signal("youtube",t[:150],f"YouTube: {ch}",url=u,score=60,meta={"ch":ch,"cats":cc}))
            except: continue
        log.info(f"YouTube: {len(out)}"); return out

class NewsFetcher:
    def fetch(self):
        out = []; seen = set()
        for topic in NEWS_TOPICS:
            try:
                feed = feedparser.parse(f"https://news.google.com/rss/search?q={quote(topic)}+when:7d&hl=en-US&gl=US&ceid=US:en")
                for e in feed.entries[:3]:
                    t = e.get("title",""); src = "News"
                    if " - " in t: t,src = t.rsplit(" - ",1)
                    k = re.sub(r'[^a-z0-9]','',t[:50].lower())
                    if k in seen: continue
                    seen.add(k)
                    if not mass_appeal(t) or not recent(e.get("published",""),7): continue
                    cc = cats(t)
                    if cc: out.append(Signal("news",t[:150],f"via {src}",url=e.get("link",""),score=70,meta={"src":src,"cats":cc}))
                time.sleep(0.15)
            except: continue
        for fn,fu in RSS_FEEDS.items():
            try:
                feed = feedparser.parse(fu)
                for e in feed.entries[:10]:
                    t = e.get("title","")
                    k = re.sub(r'[^a-z0-9]','',t[:50].lower())
                    if k in seen: continue
                    seen.add(k)
                    if not mass_appeal(t) or not recent(e.get("published",e.get("updated","")),7): continue
                    cc = cats(t)
                    if cc:
                        out.append(Signal("news",t[:150],f"via {fn}",url=e.get("link",""),score=65,meta={"src":fn,"cats":cc}))
                    elif fn in ("IGN","GameSpot","Kotaku","PC Gamer","Eurogamer","Polygon","GamesRadar","Dexerto","VG247"):
                        out.append(Signal("news",t[:150],f"via {fn}",url=e.get("link",""),score=45,meta={"src":fn,"cats":["General"]}))
                time.sleep(0.05)
            except: continue
        log.info(f"News: {len(out)}"); return out

class CompetitorFetcher:
    def fetch(self):
        out = []; hd = {"User-Agent":UA,"Accept":"text/html","Accept-Language":"en-US,en;q=0.9"}
        for name,url in COMPETITORS.items():
            try:
                r = GET(url,headers=hd,timeout=12)
                if not r: continue
                soup = BeautifulSoup(r.text,"html.parser")
                text = soup.get_text(" ",strip=True)[:5000]; promo = []
                for cat,kws in KW.items():
                    if any(k.lower() in text.lower() for k in kws) and cat not in promo: promo.append(cat)
                for tag in soup.find_all(["h1","h2","h3","title"]):
                    for c in cats(tag.get_text(strip=True)):
                        if c not in promo: promo.append(c)
                for p in promo:
                    out.append(Signal("competitor",f"{name}: {p}",f"{name} promoting {p}",url=url,score=45,meta={"comp":name,"product":p,"cats":[p]}))
                time.sleep(1)
            except: continue
        log.info(f"Competitors: {len(out)}"); return out

class CheapSharkFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=15&pageSize=30&sortBy=Deal+Rating",timeout=10)
            if r:
                for d in r.json()[:30]:
                    name,savings = d.get("title",""),float(d.get("savings",0))
                    normal,sale = float(d.get("normalPrice",0)),float(d.get("salePrice",0))
                    cc = cats(name); sc = min(40+savings*0.6,100)
                    out.append(Signal("cheapshark",name,f"${sale:.0f} (was ${normal:.0f}, {savings:.0f}% off)",
                        url=f"https://www.cheapshark.com/redirect?dealID={d.get('dealID','')}",
                        score=sc,meta={"savings":savings,"cats":cc if cc else ["Steam"]}))
        except Exception as e: log.warning(f"CheapShark: {e}")
        log.info(f"CheapShark: {len(out)}"); return out

class SteamSpyFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://steamspy.com/api.php?request=top100in2weeks",timeout=12)
            if r:
                for appid,info in sorted(r.json().items(),key=lambda x:x[1].get("ccu",0),reverse=True)[:20]:
                    name,ccu,players = info.get("name",""),info.get("ccu",0),info.get("players_2weeks",0)
                    cc = cats(name); sc = norm(ccu,500000)
                    out.append(Signal("steamspy",name,f"{ccu:,} playing now, {players:,} in 2wk",
                        url=f"https://store.steampowered.com/app/{appid}",
                        score=sc,meta={"ccu":ccu,"cats":cc if cc else ["Steam"]}))
        except Exception as e: log.warning(f"SteamSpy: {e}")
        log.info(f"SteamSpy: {len(out)}"); return out

class GamerPowerFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://www.gamerpower.com/api/giveaways?sort-by=popularity",timeout=10)
            if r:
                for g in r.json()[:25]:
                    name,platforms,worth = g.get("title",""),g.get("platforms",""),g.get("worth","N/A")
                    cc = cats(name+" "+platforms)
                    out.append(Signal("gamerpower",name,f"{g.get('type','')} on {platforms} ({worth})",
                        url=g.get("open_giveaway_url",""),score=65,
                        meta={"platforms":platforms,"cats":cc if cc else ["Gift Cards"]}))
        except Exception as e: log.warning(f"GamerPower: {e}")
        log.info(f"GamerPower: {len(out)}"); return out

class EpicFreeFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions?locale=en-US&country=US&allowCountries=US",timeout=10)
            if r:
                for el in r.json().get("data",{}).get("Catalog",{}).get("searchStore",{}).get("elements",[]):
                    name = el.get("title",""); promos = el.get("promotions")
                    if not promos: continue
                    if promos.get("promotionalOffers"):
                        out.append(Signal("epic",name,"FREE NOW on Epic",url="https://store.epicgames.com/en-US/free-games",
                            score=75,meta={"status":"free_now","cats":cats(name) or ["Fortnite"]}))
                    elif promos.get("upcomingPromotionalOffers"):
                        out.append(Signal("epic",name,"Coming free on Epic",url="https://store.epicgames.com/en-US/free-games",
                            score=55,meta={"status":"upcoming","cats":cats(name) or ["Fortnite"]}))
        except Exception as e: log.warning(f"Epic: {e}")
        log.info(f"Epic: {len(out)}"); return out

class SteamNewReleasesFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://store.steampowered.com/api/featuredcategories/",timeout=10)
            if r:
                data = r.json()
                for sec in ["new_releases","coming_soon"]:
                    for it in data.get(sec,{}).get("items",[])[:10]:
                        n,d,aid = it.get("name",""),it.get("discount_percent",0),it.get("id","")
                        label = "New Release" if sec=="new_releases" else "Coming Soon"
                        out.append(Signal("steam_new",n,f"{label}{f', {d}% off' if d else ''}",
                            url=f"https://store.steampowered.com/app/{aid}",
                            score=60 if sec=="new_releases" else 45,
                            meta={"type":sec,"cats":cats(n) or ["Steam"]}))
        except Exception as e: log.warning(f"SteamNew: {e}")
        log.info(f"SteamNew: {len(out)}"); return out

class GOGFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://www.gog.com/games/ajax/filtered?mediaType=game&page=1&sort=popularity&limit=20",
                headers={"User-Agent":UA,"Accept":"application/json"},timeout=12)
            if r:
                for g in r.json().get("products",[])[:20]:
                    name = g.get("title",""); price = g.get("price",{})
                    discount = price.get("discount",0) if isinstance(price,dict) else 0
                    slug = g.get("url","")
                    gog_url = f"https://www.gog.com{slug}" if slug else "https://www.gog.com"
                    sc = 50+(discount if isinstance(discount,int) else 0)*0.3
                    out.append(Signal("gog",name,f"GOG Popular{f', {discount}% off' if discount else ''}",
                        url=gog_url,score=min(sc,100),meta={"cats":cats(name) or ["Steam"]}))
        except Exception as e: log.warning(f"GOG: {e}")
        log.info(f"GOG: {len(out)}"); return out

class HumbleFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://www.humblebundle.com/store/api/search?sort=bestselling&filter=all&hmb_source=store_navbar&page=0",
                headers={"User-Agent":UA,"Accept":"application/json"},timeout=12)
            if r:
                for g in r.json().get("results",[])[:20]:
                    name = g.get("human_name","") or g.get("human_url","")
                    slug = g.get("human_url","")
                    cp = (g.get("current_price") or {}).get("amount",0)
                    fp = (g.get("full_price") or {}).get("amount",0)
                    discount = int((1-cp/fp)*100) if fp>0 else 0
                    url = f"https://www.humblebundle.com/store/{slug}" if slug else "https://www.humblebundle.com/store"
                    out.append(Signal("humble",name,f"Humble bestseller{f', {discount}% off' if discount>0 else ''}",
                        url=url,score=min(55+discount*0.3,100),meta={"cats":cats(name) or ["Steam"]}))
        except Exception as e: log.warning(f"Humble: {e}")
        log.info(f"Humble: {len(out)}"); return out

class FreeToGameFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://www.freetogame.com/api/games?sort-by=relevance",timeout=10)
            if r:
                for g in r.json()[:20]:
                    name,genre,platform = g.get("title",""),g.get("genre",""),g.get("platform","")
                    out.append(Signal("freetogame",name,f"Free {genre} on {platform}",
                        url=g.get("game_url",""),score=45,
                        meta={"cats":cats(name+" "+genre) or ["Gift Cards"]}))
        except Exception as e: log.warning(f"FreeToGame: {e}")
        log.info(f"FreeToGame: {len(out)}"); return out

class AnimeFetcher:
    def fetch(self):
        out = []
        try:
            r = GET("https://api.jikan.moe/v4/top/anime?filter=airing&limit=15",timeout=12)
            if r:
                for a in r.json().get("data",[]):
                    name,score,members = a.get("title",""),a.get("score",0) or 0,a.get("members",0)
                    out.append(Signal("anime",name,f"Top airing, MAL {score}, {members:,} fans",
                        url=a.get("url",""),score=norm(members,1000000),
                        meta={"cats":cats(name) or ["Crunchyroll"]}))
        except: pass
        try:
            time.sleep(1.5)
            r = GET("https://api.jikan.moe/v4/seasons/upcoming?limit=10",timeout=12)
            if r:
                for a in r.json().get("data",[]):
                    name,members = a.get("title",""),a.get("members",0)
                    out.append(Signal("anime",name,f"Upcoming, {members:,} anticipating",
                        url=a.get("url",""),score=norm(members,500000),
                        meta={"cats":cats(name) or ["Crunchyroll"]}))
        except: pass
        log.info(f"Anime: {len(out)}"); return out

# =============================================================================
# SECTION 5 - CONCURRENT ORCHESTRATOR
# =============================================================================

def fetch_all():
    print("\n" + "="*60); print("FETCHING 16 SOURCES (concurrent)"); print("="*60)
    fetchers = {
        "trends":TrendsFetcher(),"reddit":RedditFetcher(),"steam":SteamFetcher(),
        "wiki":WikiFetcher(),"youtube":YTFetcher(),"news":NewsFetcher(),
        "competitor":CompetitorFetcher(),"cheapshark":CheapSharkFetcher(),
        "steamspy":SteamSpyFetcher(),"gamerpower":GamerPowerFetcher(),
        "epic":EpicFreeFetcher(),"steam_new":SteamNewReleasesFetcher(),
        "gog":GOGFetcher(),"humble":HumbleFetcher(),
        "freetogame":FreeToGameFetcher(),"anime":AnimeFetcher(),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(f.fetch):n for n,f in fetchers.items()}
        for fut in as_completed(futs):
            n = futs[fut]
            try: results[n] = fut.result()
            except Exception as e: log.error(f"{n} crashed: {e}"); results[n] = []
    total = sum(len(v) for v in results.values())
    for n,s in sorted(results.items()): print(f"  {n}: {len(s)}")
    print(f"  TOTAL: {total}"); return results

# =============================================================================
# SECTION 6 - DEDUP & MERGE
# =============================================================================

def dedup(all_sig):
    flat = [s for sigs in all_sig.values() for s in sigs]
    if not flat: return []
    cands = []
    for sig in flat:
        merged = False
        for c in cands:
            if fuzz(sig.title, c.title): c.signals.append(sig); merged = True; break
        if not merged: cands.append(Candidate(title=sig.title, signals=[sig]))
    for c in cands:
        src_types = set(); all_cats = []
        for s in c.signals:
            st = s.source
            if st in ("news","google"): st = "news"
            if st == "steam_new": st = "steam"
            src_types.add(st)
            all_cats.extend(s.meta.get("cats",[])); all_cats.extend(cats(s.title))
        c.sources = len(src_types)
        c.categories = list(set(all_cats))
        c.category = c.categories[0] if c.categories else "General"
        c.url = _best_url(c.signals)
    print(f"  {len(flat)} signals -> {len(cands)} candidates"); return cands

# =============================================================================
# SECTION 7 - COMPOSITE SCORING  (BUG FIX: removed erroneous *100)
# =============================================================================

def comp_score(cands):
    for c in cands:
        by_src = defaultdict(list)
        for s in c.signals:
            st = s.source
            if st in ("news","google"): st = "news"
            if st == "steam_new": st = "steam"
            by_src[st].append(s.score)
        ws = 0.0; tw = 0.0
        for st,scores in by_src.items():
            w = W.get(st,0.04)
            ws += (sum(scores)/len(scores))*w; tw += w
        base = ws/tw if tw > 0 else 0
        mult = CONF.get(c.sources, CONF_DEFAULT)
        c.score = round(min(base * mult, 100), 1)
    cands.sort(key=lambda x: -x.score)
    return cands

# =============================================================================
# SECTION 8 - 4-PASS AI
# =============================================================================

def _gemini_json(prompt, retries=2):
    for i in range(retries+1):
        try:
            r = GCLIENT.models.generate_content(model=GEMINI_MODEL,contents=prompt,
                config=gtypes.GenerateContentConfig(response_mime_type="application/json",temperature=0.2))
            return json.loads(r.text.strip())
        except json.JSONDecodeError:
            if i < retries: time.sleep(2)
        except Exception as e:
            log.warning(f"Gemini JSON ({i+1}): {e}")
            if i < retries: time.sleep(3)
    return None

def _gemini_grounded(prompt, retries=2):
    for i in range(retries+1):
        try:
            r = GCLIENT.models.generate_content(model=GEMINI_MODEL,contents=prompt,
                config=gtypes.GenerateContentConfig(tools=[gtypes.Tool(google_search=gtypes.GoogleSearch())],temperature=0.3))
            sources = []
            try:
                gm = r.candidates[0].grounding_metadata
                if gm and gm.grounding_chunks:
                    for ch in gm.grounding_chunks:
                        if ch.web: sources.append({"url":ch.web.uri,"title":ch.web.title})
            except: pass
            return {"text":r.text,"sources":sources}
        except Exception as e:
            log.warning(f"Gemini grounded ({i+1}): {e}")
            if i < retries: time.sleep(3)
    return None

def pass0_ground(cands, events):
    print("\nAI PASS 0: Real-time intelligence (Google Search grounding)...")
    topics = [c.title for c in cands[:15]]
    urgent = [e["name"] for e in events[:10] if e.get("urgency") in ("critical","high")]
    prompt = f"""You are a market intelligence analyst for Recharge.com (sells gaming gift cards, streaming subscriptions, digital game credits online).
TODAY: {NOW.strftime('%B %d, %Y')}
Trending topics: {chr(10).join(f'- {t}' for t in topics)}
Imminent events: {chr(10).join(f'- {e}' for e in urgent)}
Search for the LATEST real-time information. For each with significant recent news:
1. What happened in the last 48 hours?
2. Time-sensitive opportunity for a gift card seller?
3. What are people searching for right now?
Be specific with dates and facts. Skip topics with no recent developments."""
    result = _gemini_grounded(prompt)
    if result: print(f"  Got real-time intel ({len(result.get('sources',[]))} sources)"); return result
    print("  Grounding unavailable"); return {"text":"","sources":[]}

def pass1(cands, events, ground_intel):
    print("AI PASS 1: Prioritize...")
    ct = "\n".join(f"{i+1}. [{c.score}] {c.title} (sources={c.sources}, cat={c.category})" for i,c in enumerate(cands[:30]))
    et = "\n".join(f"- {e['name']} ({e['category']}): {e['status']} - {e['description']}" for e in events[:15])
    intel = ground_intel.get("text","")[:2000]
    prompt = f"""You are a senior growth strategist at Recharge.com (gaming gift cards, streaming subscriptions, digital credits).
TODAY: {NOW.strftime('%B %d, %Y')}
REAL-TIME INTELLIGENCE: {intel if intel else "Not available"}
EVENTS (next 60 days): {et}
SCORED CANDIDATES (16 sources, composite scored): {ct}

Pick TOP 15 highest-impact opportunities. Prioritize TIME-SENSITIVE, TRENDING topics (new releases, major updates, events happening NOW or this week). Avoid generic evergreen topics. Only things that can move revenue for a gift card / digital credits platform.

Return JSON: {{"opportunities": [
  {{"title": "...", "category": "...", "urgency": "critical|high|medium",
    "confidence": 0.0-1.0, "why_now": "1 sentence", "revenue_signal": "how this drives purchases, 1 sentence"}}
]}}

IMPORTANT: Use EXACT candidate titles from the list above. Do NOT rephrase them.
Each must be specific and time-bound. Spread across categories."""
    r = _gemini_json(prompt)
    if r and "opportunities" in r: print(f"  {len(r['opportunities'])} opportunities"); return r["opportunities"]
    return [{"title":c.title,"category":c.category,"urgency":"high" if c.score>50 else "medium",
             "confidence":min(c.score/100,1.0),"why_now":f"Across {c.sources} sources",
             "revenue_signal":"Multiple signals indicate purchase intent"} for c in cands[:15]]

def pass2(opps, comp_signals):
    print("AI PASS 2: SEO briefs...")
    ct = "\n".join(f"- {s.title}: {s.desc}" for s in comp_signals[:20])
    prompt = f"""You are Head of SEO at Recharge.com (gaming gift cards, streaming subscriptions, digital credits).
OPPORTUNITIES: {json.dumps(opps[:15],indent=2)}
COMPETITOR INTEL: {ct if ct else "None available"}

For each, create a content brief the SEO team can execute immediately.
Return JSON: {{"briefs": [{{"title":"...","headline":"SEO H1","keywords":["5 keywords"],"actions":["3 actions"],"cta":"CTA text"}}]}}
Headlines must target high-volume search queries. Actions must be specific."""
    r = _gemini_json(prompt)
    if r and "briefs" in r: print(f"  {len(r['briefs'])} briefs"); return r["briefs"]
    return [{"title":o.get("title",""),"headline":o.get("title",""),"keywords":[o.get("category","")],
             "actions":["Create landing page"],"cta":"Buy on Recharge.com"} for o in opps[:15]]

def pass3(opps, briefs, ground_intel):
    print("AI PASS 3: Executive synthesis (grounded)...")
    prompt = f"""You are VP Growth presenting to C-suite at Recharge.com (gaming gift cards, streaming subs, digital credits).
VERIFIED OPPORTUNITIES: {json.dumps(opps[:10],indent=2)}
SEO BRIEFS: {json.dumps(briefs[:8],indent=2)}

Search the web for latest gaming/streaming news. Write executive briefing. Every sentence must earn its place.
Return JSON: {{"summary":"3 sentences max, lead with #1 revenue opportunity",
"actions":["SEO Team: do X this week","Content Team: do Y this week","Marketing Team: do Z this week"],
"predictions":["2-3 specific trends next 1-2 weeks"],"risks":["2-3 risks to watch"]}}
IMPORTANT: Each action must be a plain STRING like "SEO Team: Update landing pages for X - due THIS WEEK". Do NOT return objects/dicts."""
    result = _gemini_grounded(prompt)
    if result and result.get("text"):
        try:
            parsed = json.loads(result["text"])
            if "summary" in parsed: parsed["_sources"] = result.get("sources",[]); print("  Done (grounded)"); return parsed
        except: pass
    r = _gemini_json(prompt)
    if r and "summary" in r: print("  Done"); return r
    return {"summary":"Multiple revenue opportunities identified.","actions":["SEO team: act on top opportunities",
            "Content team: update landing pages","Marketing team: monitor competitors"],
            "predictions":["Watch for major updates"],"risks":["Competitor pricing"]}

def run_ai(cands, events, comp_sigs):
    print("\n" + "="*60); print("4-PASS AI ANALYSIS (with Google Search grounding)"); print("="*60)
    ground = pass0_ground(cands, events)
    opps = pass1(cands, events, ground)
    briefs = pass2(opps, comp_sigs)
    ex = pass3(opps, briefs, ground)
    return {"opportunities":opps,"briefs":briefs,"executive":ex,"ground_intel":ground}

# =============================================================================
# SECTION 9 - GOOGLE SHEETS
# =============================================================================

def write_sheets(cands, ai, events):
    if not IS_COLAB: print("\nGOOGLE SHEETS... skipped (not Colab)"); return None
    print("\nGOOGLE SHEETS...", end=" ")
    try:
        from google.colab import auth; auth.authenticate_user()
        creds,_ = _gauth(); gc = gspread.authorize(creds)
    except:
        if not HAS_GSPREAD: print("skipped"); return None
        try: gc = gspread.service_account()
        except: print("auth failed"); return None
    try: sh = gc.create(f"Recharge Scanner {DATE}")
    except Exception as e: print(f"failed: {e}"); return None
    try:
        ex = ai.get("executive",{})
        ws1 = sh.sheet1; ws1.update_title("Dashboard")
        ws1.update(values=[["RECHARGE OPPORTUNITY SCANNER",DATE],[""],["Metric","Value"],
            ["Candidates",str(len(cands))],["Multi-source",str(len([c for c in cands if c.sources>=2]))],
            ["Events",str(len(events))],[""],["SUMMARY"],[ex.get("summary","")],
            [""],["ACTIONS"],*[[a] for a in ex.get("actions",[])]],range_name="A1")
        ws2 = sh.add_worksheet("Opportunities",100,10)
        rows = [["#","Title","Category","Score","Sources","Urgency","Revenue Signal","Source URL"]]
        for i,o in enumerate(ai.get("opportunities",[])[:20],1):
            mc = _match_cand(o.get("title",""),cands)
            rows.append([str(i),o.get("title",""),o.get("category",""),
                str(mc.score) if mc else "-",str(mc.sources) if mc else "-",
                o.get("urgency",""),o.get("revenue_signal",""),mc.url if mc else ""])
        ws2.update(values=rows,range_name="A1")
        ws3 = sh.add_worksheet("SEO Briefs",100,5)
        brows = [["Title","Headline","Keywords","Actions","CTA"]]
        for b in ai.get("briefs",[]): brows.append([b.get("title",""),b.get("headline",""),
            ", ".join(b.get("keywords",[]))," | ".join(b.get("actions",[])),b.get("cta","")])
        ws3.update(values=brows,range_name="A1")
        ws4 = sh.add_worksheet("History",200,5)
        hrows = [["Date","Title","Score","Sources","Category"]]
        for c in cands[:30]: hrows.append([DATE,c.title[:80],str(c.score),str(c.sources),c.category])
        ws4.update(values=hrows,range_name="A1")
        print(f"OK ({sh.url})"); return sh.url
    except Exception as e: log.error(f"Sheets: {e}"); return None

# =============================================================================
# SECTION 10 - HTML DASHBOARD
# =============================================================================

def build_html(cands, ai, events, all_sig):
    print("\nHTML DASHBOARD...", end=" ")
    ex = ai.get("executive",{}); opps = ai.get("opportunities",[]); briefs = ai.get("briefs",[])
    ground = ai.get("ground_intel",{})
    total_sig = sum(len(v) for v in all_sig.values())
    multi = len([c for c in cands if c.sources>=2])
    n_cats = len(set(c.category for c in cands))
    crit_ev = len([e for e in events if e.get("urgency")=="critical"])
    n_sources = len([k for k,v in all_sig.items() if len(v)>0])
    urg_crit = len([o for o in opps if o.get("urgency")=="critical"])
    urg_high = len([o for o in opps if o.get("urgency")=="high"])

    cat_count = defaultdict(int)
    for c in cands: cat_count[c.category] += 1
    top_cats = sorted(cat_count.items(),key=lambda x:-x[1])[:10]
    cat_labels = json.dumps([c[0] for c in top_cats]); cat_values = json.dumps([c[1] for c in top_cats])
    active_sources = {k:len(v) for k,v in all_sig.items() if len(v)>0}
    src_labels = json.dumps(list(active_sources.keys())); src_values = json.dumps(list(active_sources.values()))

    score_data = []
    for o in opps[:15]:
        mc = _match_cand(o.get("title",""),cands)
        score_data.append({"label":o.get("title","")[:40],"score":mc.score if mc else 0})
    score_labels = json.dumps([d["label"] for d in score_data])
    score_values = json.dumps([d["score"] for d in score_data])

    opp_rows = ""
    for i,o in enumerate(opps[:15],1):
        mc = _match_cand(o.get("title",""),cands)
        sc = f"{mc.score}" if mc else "-"; sr = str(mc.sources) if mc else "-"; url = mc.url if mc else ""
        urg = o.get("urgency",""); urg_cls = {"critical":"urg-crit","high":"urg-high","medium":"urg-med"}.get(urg,"urg-med")
        title_html = f'<a href="{esc(url)}" target="_blank" rel="noopener">{esc(o.get("title",""))}</a>' if url else esc(o.get("title",""))
        dom = _domain(url)
        src_html = f'<a href="{esc(url)}" target="_blank" class="src-link">{esc(dom)}</a>' if dom else '<span class="t2">-</span>'
        opp_rows += f"""<tr><td class="rank">{i}</td><td class="opp-title">{title_html}</td>
<td><span class="cat-tag">{esc(o.get('category',''))}</span></td><td class="score-val">{sc}</td><td>{sr}</td>
<td><span class="badge {urg_cls}">{urg.upper()}</span></td><td class="rev-sig">{esc(o.get('revenue_signal',''))[:80]}</td>
<td class="src-cell">{src_html}</td></tr>"""

    briefs_html = ""
    for b in briefs[:10]:
        kws = ", ".join(b.get("keywords",[])); acts = "".join(f"<li>{esc(a)}</li>" for a in b.get("actions",[]))
        briefs_html += f"""<div class="brief-card"><div class="brief-head">{esc(b.get('title',''))}</div>
<div class="brief-field"><span class="bf-label">H1</span> {esc(b.get('headline',''))}</div>
<div class="brief-field"><span class="bf-label">Keywords</span> <span class="kw">{esc(kws)}</span></div>
<div class="brief-field"><span class="bf-label">CTA</span> {esc(b.get('cta',''))}</div>
<div class="brief-actions"><span class="bf-label">Actions</span><ul>{acts}</ul></div></div>"""

    events_rows = ""
    for e in events[:20]:
        urg = e.get("urgency",""); urg_cls = {"critical":"urg-crit","high":"urg-high","medium":"urg-med"}.get(urg,"")
        live = ' <span class="badge badge-live">LIVE</span>' if e.get("is_live") else ""
        events_rows += f"""<tr><td>{esc(e['name'][:55])}{live}</td><td><span class="cat-tag">{esc(e['category'])}</span></td>
<td><span class="badge {urg_cls}">{esc(e['status'])}</span></td><td>{esc(e['description'][:50])}</td></tr>"""

    comp_by = defaultdict(list)
    for s in all_sig.get("competitor",[]):
        p = s.meta.get("product","")
        if p and p not in comp_by[s.meta.get("comp","?")]: comp_by[s.meta.get("comp","?")].append(p)
    comp_html = "".join(f'<div class="comp-row"><strong>{esc(cn)}</strong>{"".join(f"""<span class="pill">{esc(p)}</span>""" for p in prods[:8])}</div>' for cn,prods in comp_by.items())

    g_sources = ground.get("sources",[])
    sources_html = ""
    if g_sources:
        sources_html = '<div class="ground-sources"><h4>Verified Sources</h4><ul>' + "".join(
            f'<li><a href="{esc(gs.get("url",""))}" target="_blank">{esc(gs.get("title","Source"))}</a></li>' for gs in g_sources[:8]) + '</ul></div>'

    pred_html = "".join(f"<li>{esc(p)}</li>" for p in ex.get("predictions",[]))
    risk_html = "".join(f"<li>{esc(r)}</li>" for r in ex.get("risks",[]))

    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Recharge.com Opportunity Scanner</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--bg:#0b0d11;--card:#13161f;--card2:#191d29;--border:#242938;--t:#e2e4ea;--t2:#7c819a;--accent:#818cf8;--green:#34d399;--red:#f87171;--amber:#fbbf24;--blue:#60a5fa;--purple:#c084fc}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif;background:var(--bg);color:var(--t);line-height:1.55;font-size:14px}}
.wrap{{max-width:1440px;margin:0 auto;padding:20px 24px}}
header{{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:28px;padding-bottom:14px;border-bottom:1px solid var(--border)}}
header h1{{font-size:20px;font-weight:700}}header h1 span{{color:var(--accent)}}
.meta{{color:var(--t2);font-size:12px;text-align:right}}.meta strong{{color:var(--green)}}
.kpis{{display:grid;grid-template-columns:repeat(7,1fr);gap:12px;margin-bottom:24px}}
.kpi{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px}}
.kpi .lb{{font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}}
.kpi .vl{{font-size:26px;font-weight:700}}
.kpi .vl.green{{color:var(--green)}}.kpi .vl.amber{{color:var(--amber)}}.kpi .vl.red{{color:var(--red)}}
.kpi .vl.blue{{color:var(--blue)}}.kpi .vl.purple{{color:var(--purple)}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px}}
.card h2{{font-size:15px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.card h3{{font-size:13px;font-weight:600;margin:14px 0 6px;color:var(--t2)}}
.summary-text{{font-size:15px;line-height:1.7}}
.action-item{{padding:7px 0;border-bottom:1px solid var(--border);font-size:13px}}.action-item:last-child{{border:none}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;padding:8px 10px;background:var(--card2);color:var(--t2);font-weight:600;text-transform:uppercase;font-size:10.5px;letter-spacing:.5px;border-bottom:2px solid var(--border)}}
td{{padding:9px 10px;border-bottom:1px solid var(--border)}}tr:hover{{background:rgba(129,140,248,.03)}}
.rank{{color:var(--t2);font-weight:600;width:30px}}.opp-title{{font-weight:600;max-width:300px}}
.opp-title a{{color:var(--t);text-decoration:none;border-bottom:1px dotted var(--t2);transition:all .15s}}
.opp-title a:hover{{color:var(--accent);border-color:var(--accent)}}
.score-val{{font-weight:700;color:var(--accent);font-size:15px}}
.rev-sig{{color:var(--t2);font-size:12px;max-width:260px}}.src-cell{{font-size:12px}}
.src-link{{color:var(--blue);text-decoration:none;font-size:11px}}.src-link:hover{{text-decoration:underline}}
.t2{{color:var(--t2)}}
.cat-tag{{background:rgba(129,140,248,.1);color:var(--accent);padding:2px 8px;border-radius:6px;font-size:11px;white-space:nowrap}}
.badge{{display:inline-block;padding:2px 10px;border-radius:20px;font-size:10.5px;font-weight:600;text-transform:uppercase;letter-spacing:.3px}}
.urg-crit{{background:rgba(248,113,113,.12);color:var(--red)}}.urg-high{{background:rgba(251,191,36,.12);color:var(--amber)}}
.urg-med{{background:rgba(129,140,248,.12);color:var(--accent)}}
.badge-live{{background:rgba(248,113,113,.18);color:var(--red);animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
.chart-row{{display:grid;gap:16px;margin-bottom:20px}}.chart-hero{{grid-template-columns:1fr}}.chart-pair{{grid-template-columns:1fr 1fr}}
.chart-box{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:18px}}
.chart-box h3{{font-size:12px;color:var(--t2);margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px;border:none;padding:0}}
.chart-container{{position:relative;height:380px}}
.brief-card{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:10px}}
.brief-head{{font-size:13px;font-weight:600;color:var(--accent);margin-bottom:8px}}
.brief-field{{font-size:12.5px;margin-bottom:4px;display:flex;gap:6px}}
.bf-label{{color:var(--t2);min-width:70px;font-weight:600;flex-shrink:0}}
.brief-actions{{font-size:12.5px}}.brief-actions ul{{padding-left:16px;margin-top:4px}}.brief-actions li{{margin-bottom:3px}}
.kw{{color:var(--green)}}
.comp-row{{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:6px}}
.pill{{background:rgba(129,140,248,.08);color:var(--accent);padding:2px 10px;border-radius:12px;font-size:11px}}
.ground-sources{{margin-top:12px;padding-top:10px;border-top:1px solid var(--border)}}
.ground-sources h4{{font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}}
.ground-sources ul{{list-style:none;padding:0}}.ground-sources li{{font-size:12px;margin-bottom:3px}}
.ground-sources a{{color:var(--blue);text-decoration:none}}.ground-sources a:hover{{text-decoration:underline}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}.two-col .card{{margin-bottom:0}}
@media print{{body{{background:#fff;color:#111}}.card,.kpi{{border:1px solid #ddd}}}}
@media(max-width:1000px){{.kpis{{grid-template-columns:repeat(3,1fr)}}.chart-pair,.two-col{{grid-template-columns:1fr}}}}
</style></head><body><div class="wrap">
<header><h1><span>Recharge.com</span> Opportunity Scanner</h1>
<div class="meta">v4.2 &middot; {DATE} {TIME}<br><strong>{n_sources} sources</strong> &middot; 4-pass AI (grounded)</div></header>
<div class="kpis">
<div class="kpi"><div class="lb">Signals</div><div class="vl blue">{total_sig}</div></div>
<div class="kpi"><div class="lb">Candidates</div><div class="vl">{len(cands)}</div></div>
<div class="kpi"><div class="lb">Multi-Source</div><div class="vl purple">{multi}</div></div>
<div class="kpi"><div class="lb">Categories</div><div class="vl green">{n_cats}</div></div>
<div class="kpi"><div class="lb">Critical Events</div><div class="vl red">{crit_ev}</div></div>
<div class="kpi"><div class="lb">Critical Opps</div><div class="vl red">{urg_crit}</div></div>
<div class="kpi"><div class="lb">High Priority</div><div class="vl amber">{urg_high}</div></div>
</div>
<div class="card"><h2>Executive Summary</h2><p class="summary-text">{esc(ex.get('summary',''))}</p>
{sources_html}</div>
<div class="chart-row chart-hero"><div class="chart-box"><h3>Top Opportunities by Composite Score</h3>
<div class="chart-container"><canvas id="scoreChart"></canvas></div></div></div>
<div class="chart-row chart-pair">
<div class="chart-box"><h3>Signals by Source</h3><canvas id="srcChart"></canvas></div>
<div class="chart-box"><h3>Candidates by Category</h3><canvas id="catChart"></canvas></div></div>
<div class="card"><h2>Top Opportunities</h2><table><thead><tr>
<th>#</th><th>Opportunity</th><th>Category</th><th>Score</th><th>Sources</th><th>Urgency</th><th>Revenue Signal</th><th>Source</th>
</tr></thead><tbody>{opp_rows}</tbody></table></div>
<div class="card"><h2>SEO Content Briefs</h2>{briefs_html}</div>
<div class="two-col">
<div class="card"><h2>Competitor Positioning</h2>{comp_html if comp_html else '<p class="t2">No competitor data.</p>'}</div>
<div class="card"><h2>Outlook</h2><h3>Predicted Trends</h3><ul style="font-size:13px;padding-left:16px">{pred_html}</ul>
<h3>Risk Watchlist</h3><ul style="font-size:13px;padding-left:16px">{risk_html}</ul></div></div>
<div class="card"><h2>Events Calendar</h2><table><thead><tr><th>Event</th><th>Category</th><th>Status</th><th>Details</th></tr></thead>
<tbody>{events_rows}</tbody></table></div>
<footer style="text-align:center;padding:20px;color:var(--t2);font-size:11px">
Recharge.com v4.2 &middot; {n_sources} sources &middot; {total_sig} signals &middot; Gemini (grounded)</footer></div>
<script>
const C=['#818cf8','#60a5fa','#34d399','#fbbf24','#f87171','#c084fc','#fb923c','#2dd4bf','#a78bfa','#f472b6','#38bdf8','#4ade80','#e879f9','#67e8f9','#bef264'];
Chart.defaults.color='#7c819a';Chart.defaults.borderColor='#242938';
new Chart(document.getElementById('scoreChart'),{{type:'bar',
data:{{labels:{score_labels},datasets:[{{data:{score_values},backgroundColor:C.slice(0,15),borderRadius:6,borderSkipped:false,barPercentage:0.55}}]}},
options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,
plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},max:100}},y:{{grid:{{display:false}},ticks:{{font:{{size:11,weight:600}}}}}}}}
}}}});
new Chart(document.getElementById('srcChart'),{{type:'doughnut',
data:{{labels:{src_labels},datasets:[{{data:{src_values},backgroundColor:C,borderWidth:0}}]}},
options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{boxWidth:10,padding:6,font:{{size:11}}}}}}}},cutout:'60%'}}}});
new Chart(document.getElementById('catChart'),{{type:'bar',
data:{{labels:{cat_labels},datasets:[{{data:{cat_values},backgroundColor:'#818cf8',borderRadius:5,borderSkipped:false}}]}},
options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}},maxRotation:45}}}},y:{{grid:{{display:false}}}}}}}}}});
</script></body></html>"""
    # ---- password protection wrapper ----
    if DASH_PASSWORD:
        import hashlib, base64
        pw_hash = hashlib.sha256(DASH_PASSWORD.encode()).hexdigest()
        # XOR-encrypt the HTML with the password so content isn't in page source
        pw_bytes = DASH_PASSWORD.encode()
        html_bytes = html.encode("utf-8")
        encrypted = bytes(b ^ pw_bytes[i % len(pw_bytes)] for i, b in enumerate(html_bytes))
        enc_b64 = base64.b64encode(encrypted).decode()
        html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Recharge.com Dashboard - Login</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0b0d11;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e2e4ea;display:flex;align-items:center;justify-content:center;min-height:100vh}}
.login-box{{background:#13161f;border:1px solid #242938;border-radius:16px;padding:40px;width:380px;text-align:center}}
.login-box h1{{font-size:20px;margin-bottom:6px}}.login-box h1 span{{color:#818cf8}}
.login-box p{{color:#7c819a;font-size:13px;margin-bottom:24px}}
.pw-input{{width:100%;padding:12px 16px;background:#0b0d11;border:1px solid #242938;border-radius:8px;color:#e2e4ea;font-size:14px;margin-bottom:12px;outline:none}}
.pw-input:focus{{border-color:#818cf8}}
.pw-btn{{width:100%;padding:12px;background:#818cf8;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer}}
.pw-btn:hover{{background:#6366f1}}
.pw-err{{color:#f87171;font-size:12px;margin-top:8px;display:none}}
</style></head><body>
<div class="login-box" id="loginBox">
<h1><span>Recharge.com</span> Scanner</h1>
<p>Enter password to view the dashboard</p>
<input type="password" class="pw-input" id="pwInput" placeholder="Password" onkeydown="if(event.key==='Enter')unlock()">
<button class="pw-btn" onclick="unlock()">Unlock Dashboard</button>
<div class="pw-err" id="pwErr">Incorrect password</div>
</div>
<script>
const H="{pw_hash}",D="{enc_b64}";
async function sha256(s){{const e=new TextEncoder().encode(s);const h=await crypto.subtle.digest('SHA-256',e);return Array.from(new Uint8Array(h)).map(b=>b.toString(16).padStart(2,'0')).join('')}}
async function unlock(){{const pw=document.getElementById('pwInput').value;const h=await sha256(pw);if(h!==H){{document.getElementById('pwErr').style.display='block';return}}
const enc=Uint8Array.from(atob(D),c=>c.charCodeAt(0));const pwB=new TextEncoder().encode(pw);const dec=new Uint8Array(enc.length);for(let i=0;i<enc.length;i++)dec[i]=enc[i]^pwB[i%pwB.length];
document.open();document.write(new TextDecoder().decode(dec));document.close()}}
</script></body></html>"""

    fname = f"recharge_dashboard_{DATE}.html"
    with open(fname,"w",encoding="utf-8") as f: f.write(html)
    with open("index.html","w",encoding="utf-8") as f: f.write(html)
    print(f"OK -> {fname} + index.html"); return fname

# =============================================================================
# SECTION 11 - WORD DOCUMENT
# =============================================================================

def _shade(cell, color):
    s = OxmlElement('w:shd'); s.set(qn('w:fill'),color)
    cell._tc.get_or_add_tcPr().append(s)

def build_docx(cands, ai, events, all_sig):
    print("\nWORD DOCUMENT...", end=" ")
    doc = Document(); ex = ai.get("executive",{}); opps = ai.get("opportunities",[]); briefs = ai.get("briefs",[])
    n_sources = len([k for k,v in all_sig.items() if len(v)>0])
    t = doc.add_heading("Recharge.com Opportunity Report",0); t.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(f"{DATE} {TIME} | {n_sources} sources | 4-pass AI"); r.font.color.rgb = RGBColor(128,128,128); r.font.size = Pt(10)
    doc.add_heading("Executive Summary",level=1); doc.add_paragraph(ex.get("summary",""))
    doc.add_heading("Top Opportunities",level=1)
    tbl = doc.add_table(rows=1,cols=7); tbl.style = "Table Grid"
    for i,h in enumerate(["#","Opportunity","Category","Score","Src","Urgency","Revenue Signal"]):
        c2 = tbl.rows[0].cells[i]; c2.text = h; c2.paragraphs[0].runs[0].bold = True
        _shade(c2,"1a237e"); c2.paragraphs[0].runs[0].font.color.rgb = RGBColor(255,255,255)
    for i,o in enumerate(opps[:15],1):
        row = tbl.add_row().cells; mc = _match_cand(o.get("title",""),cands)
        row[0].text = str(i); row[1].text = o.get("title","")[:45]; row[2].text = o.get("category","")
        row[3].text = str(mc.score) if mc else "-"; row[4].text = str(mc.sources) if mc else "-"
        row[5].text = o.get("urgency",""); row[6].text = o.get("revenue_signal","")[:60]
        u = o.get("urgency","")
        if u == "critical": _shade(row[5],"ffcdd2")
        elif u == "high": _shade(row[5],"fff9c4")
    doc.add_heading("SEO Content Briefs",level=1)
    for b in briefs[:10]:
        doc.add_heading(b.get("title","")[:50],level=2); p = doc.add_paragraph()
        p.add_run("H1: ").bold = True; p.add_run(b.get("headline","") + "\n")
        p.add_run("Keywords: ").bold = True; p.add_run(", ".join(b.get("keywords",[])) + "\n")
        p.add_run("CTA: ").bold = True; p.add_run(b.get("cta","") + "\n")
        p.add_run("Actions: ").bold = True; p.add_run(" | ".join(b.get("actions",[])))
    doc.add_heading("Events (next 60 days)",level=1)
    tbl = doc.add_table(rows=1,cols=4); tbl.style = "Table Grid"
    for i,h in enumerate(["Event","Category","Status","Details"]):
        c2 = tbl.rows[0].cells[i]; c2.text = h; c2.paragraphs[0].runs[0].bold = True
        _shade(c2,"1a237e"); c2.paragraphs[0].runs[0].font.color.rgb = RGBColor(255,255,255)
    for e in events[:20]:
        row = tbl.add_row().cells; row[0].text = e["name"][:50]; row[1].text = e["category"]
        row[2].text = e["status"]; row[3].text = e["description"][:45]
        if e.get("urgency")=="critical": _shade(row[2],"ffcdd2")
        elif e.get("urgency")=="high": _shade(row[2],"fff9c4")
    for title,key in [("Predictions","predictions"),("Risk Watchlist","risks")]:
        items = ex.get(key,[])
        if items:
            doc.add_heading(title,level=1)
            for item in items: doc.add_paragraph(item,style="List Bullet")
    fname = f"recharge_report_v4_{DATE}.docx"; doc.save(fname); print(f"OK -> {fname}"); return fname

# =============================================================================
# SECTION 12 - EMAIL
# =============================================================================

def build_email_html(cands, ai, events, all_sig, dashboard_url=""):
    ex = ai.get("executive",{}); opps = ai.get("opportunities",[])
    n_sources = len([k for k,v in all_sig.items() if len(v)>0])
    total_sig = sum(len(v) for v in all_sig.values())
    opp_rows = ""
    for i, o in enumerate(opps[:10], 1):
        mc = _match_cand(o.get("title",""),cands)
        sc = str(mc.score) if mc else "-"; url = mc.url if mc else ""
        urg = o.get("urgency","")
        urg_bg = {"critical":"#fecaca","high":"#fef3c7","medium":"#e0e7ff"}.get(urg,"#e0e7ff")
        urg_color = {"critical":"#dc2626","high":"#d97706","medium":"#4f46e5"}.get(urg,"#4f46e5")
        t_html = f'<a href="{esc(url)}" style="color:#2563eb;text-decoration:none">{esc(o.get("title",""))}</a>' if url else esc(o.get("title",""))
        dom = _domain(url)
        src_td = f'<a href="{esc(url)}" style="color:#6b7280;font-size:11px;text-decoration:none">{esc(dom)}</a>' if dom else '-'
        opp_rows += f"""<tr><td style="padding:10px 8px;border-bottom:1px solid #e5e7eb;text-align:center;color:#6b7280;font-weight:600">{i}</td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb;font-weight:600">{t_html}</td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb">{esc(o.get("category",""))}</td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb;text-align:center;font-weight:700;color:#4f46e5;font-size:16px">{sc}</td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb;text-align:center"><span style="background:{urg_bg};color:{urg_color};padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;text-transform:uppercase">{urg}</span></td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;font-size:12px">{esc(o.get("revenue_signal",""))[:55]}</td>
<td style="padding:10px 8px;border-bottom:1px solid #e5e7eb">{src_td}</td></tr>"""
    dash_btn = f'<tr><td style="padding:24px 32px;text-align:center"><a href="{esc(dashboard_url)}" style="display:inline-block;background:#4f46e5;color:#ffffff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px">View Full Dashboard &rarr;</a></td></tr>' if dashboard_url else ""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:20px 0"><tr><td align="center">
<table width="700" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">
<tr><td style="background:#1e1b4b;padding:28px 32px"><h1 style="margin:0;color:#ffffff;font-size:20px;font-weight:700">Recharge.com <span style="color:#a5b4fc">Opportunity Scanner</span></h1>
<p style="margin:8px 0 0;color:#a5b4fc;font-size:12px">{DATE} | {n_sources} sources | {total_sig} signals</p></td></tr>
<tr><td style="padding:28px 32px"><h2 style="margin:0 0 12px;font-size:16px;color:#1f2937">Executive Summary</h2>
<p style="margin:0;font-size:15px;line-height:1.7;color:#374151">{esc(ex.get("summary",""))}</p></td></tr>
<tr><td style="padding:0 32px 24px"><h2 style="margin:0 0 12px;font-size:16px;color:#1f2937">Top 10 Opportunities</h2>
<table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;font-size:13px">
<tr style="background:#f9fafb"><th style="padding:10px 8px;text-align:center;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">#</th>
<th style="padding:10px 8px;text-align:left;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Opportunity</th>
<th style="padding:10px 8px;text-align:left;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Category</th>
<th style="padding:10px 8px;text-align:center;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Score</th>
<th style="padding:10px 8px;text-align:center;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Urgency</th>
<th style="padding:10px 8px;text-align:left;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Signal</th>
<th style="padding:10px 8px;text-align:left;font-size:10px;color:#6b7280;text-transform:uppercase;border-bottom:2px solid #e5e7eb">Source</th></tr>
{opp_rows}</table></td></tr>{dash_btn}
<tr><td style="padding:20px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;text-align:center">
<p style="margin:0;font-size:11px;color:#9ca3af">Recharge.com Opportunity Scanner v4.2 | Auto-generated weekly</p></td></tr>
</table></td></tr></table></body></html>"""

def send_email(html_body, subject=None):
    to_addr = os.environ.get("EMAIL_TO",""); from_addr = os.environ.get("EMAIL_FROM","")
    smtp_host = os.environ.get("SMTP_HOST","smtp.gmail.com"); smtp_port = int(os.environ.get("SMTP_PORT","587"))
    smtp_user = os.environ.get("SMTP_USER",""); smtp_pass = os.environ.get("SMTP_PASS","")
    missing = [n for n,v in [("EMAIL_TO",to_addr),("EMAIL_FROM",from_addr),("SMTP_USER",smtp_user),("SMTP_PASS",smtp_pass)] if not v]
    if missing: print(f"EMAIL SEND... skipped (missing secrets: {', '.join(missing)})"); return False
    import smtplib; from email.mime.multipart import MIMEMultipart; from email.mime.text import MIMEText
    if not subject: subject = f"Recharge.com Scanner | {DATE} | Weekly Opportunity Report"
    msg = MIMEMultipart('alternative'); msg['Subject'] = subject; msg['From'] = from_addr; msg['To'] = to_addr
    msg.attach(MIMEText(html_body,'html'))
    try:
        with smtplib.SMTP(smtp_host,smtp_port) as server:
            server.starttls(); server.login(smtp_user,smtp_pass)
            server.sendmail(from_addr,[a.strip() for a in to_addr.split(",")],msg.as_string())
        print(f"EMAIL SEND... OK -> {to_addr}"); return True
    except Exception as e: print(f"EMAIL SEND... failed: {e}"); return False

# =============================================================================
# SECTION 13 - EVENTS CALENDAR
# =============================================================================

def get_events():
    today = NOW
    cal = [
        (2,27,28,"Resident Evil Requiem","Game Release","PS5/XSX/Switch 2/PC",9),
        (3,19,20,"Crimson Desert","Game Release","PS5/XSX/PC",8),
        (3,20,21,"Saros (Housemarque)","PlayStation","PS5 exclusive",8),
        (3,27,28,"007 First Light","Game Release","James Bond",8),
        (3,15,31,"Marathon Launch","PlayStation","Bungie extraction shooter",9),
        (8,15,16,"Madden NFL 27","Game Release","Annual",8),
        (9,1,7,"NBA 2K27","Game Release","Annual",8),
        (9,25,30,"EA Sports FC 27","EA Sports FC","New FC game",10),
        (10,15,31,"Call of Duty 2026","Call of Duty","First on Switch 2",10),
        (11,19,19,"GTA 6 LAUNCH","GTA","Biggest release in history",10),
        (6,1,30,"Fable","Xbox","Xbox exclusive RPG",9),
        (10,1,31,"Marvel's Wolverine","PlayStation","Insomniac PS5",9),
        (12,1,31,"Forza Horizon 6","Xbox","Set in Japan",8),
        (1,22,22,"Xbox Developer Direct","Xbox","Confirmed",9),
        (6,7,8,"Xbox Games Showcase","Xbox","Post-SGF",9),
        (1,1,7,"Game Pass Wave 1","Xbox","New additions",7),
        (2,1,7,"Game Pass Feb","Xbox","New additions",7),
        (3,1,7,"Game Pass Mar","Xbox","New additions",7),
        (2,15,28,"State of Play","PlayStation","Feb broadcast",8),
        (5,25,31,"PlayStation Showcase","PlayStation","Major",9),
        (9,15,25,"State of Play Fall","PlayStation","Sep",8),
        (1,1,7,"PS Plus Jan","PlayStation","Monthly free games",8),
        (2,1,7,"PS Plus Feb","PlayStation","Monthly free games",8),
        (3,1,7,"PS Plus Mar","PlayStation","Monthly free games",8),
        (4,1,7,"PS Plus Apr","PlayStation","Monthly free games",8),
        (5,1,7,"PS Plus May","PlayStation","Monthly free games",8),
        (6,1,7,"PS Plus Jun","PlayStation","Monthly free games",8),
        (2,10,20,"Nintendo Direct Feb","Nintendo","Annual pattern",8),
        (6,10,15,"Nintendo Direct Summer","Nintendo","SGF period",9),
        (9,10,20,"Nintendo Direct Fall","Nintendo","Sep pattern",8),
        (2,23,23,"Steam Next Fest","Steam","Feb 23-Mar 2",7),
        (3,19,19,"Steam Spring Sale","Steam","Mar 19-26",9),
        (6,26,26,"Steam Summer Sale","Steam","Biggest sale",10),
        (11,25,25,"Steam Autumn Sale","Steam","Pre-BF",9),
        (12,18,18,"Steam Winter Sale","Steam","Through Jan 2",10),
        (1,12,19,"Steam Detective Fest","Steam","Mystery games",8),
        (2,9,16,"Steam PvP Fest","Steam","Competitive",8),
        (4,20,27,"Steam Medieval Fest","Steam","Knights & castles",8),
        (5,4,11,"Steam Deckbuilders Fest","Steam","Card strategy",8),
        (8,3,10,"Steam Cyberpunk Fest","Steam","Neon dystopian",8),
        (10,19,26,"Steam Next Fest Oct","Steam","Fall demos",9),
        (10,26,31,"Steam Scream V","Steam","Halloween horror",9),
        (1,10,25,"EA FC TOTY","EA Sports FC","Team of the Year",10),
        (2,5,20,"EA FC Future Stars","EA Sports FC","Young talents",8),
        (3,15,31,"FUT Birthday","EA Sports FC","Anniversary",8),
        (5,1,31,"EA FC TOTS","EA Sports FC","Team of the Season",9),
        (6,15,30,"EA FC Futties","EA Sports FC","End of cycle",8),
        (11,20,30,"EA FC Black Friday","EA Sports FC","Lightning rounds",9),
        (3,9,13,"GDC","Esports","San Francisco",7),
        (3,26,29,"PAX East","Esports","Boston",7),
        (4,27,30,"iicon (E3 successor)","Esports","Las Vegas",9),
        (6,5,8,"Summer Game Fest","Esports","LA",10),
        (8,26,30,"Gamescom","Esports","Cologne",9),
        (9,12,13,"BlizzCon","Esports","Anaheim",8),
        (9,17,21,"Tokyo Game Show","Esports","Chiba",8),
        (12,5,12,"The Game Awards","Esports","Major reveals",9),
        (7,6,6,"Esports World Cup","Esports","Riyadh $70M+",9),
        (10,1,31,"LoL Worlds","Esports","NYC",9),
        (1,14,14,"Genshin 6.3","Genshin Impact","Lantern Rite",9),
        (2,25,25,"Genshin 6.4","Genshin Impact","Varka banner",9),
        (4,8,8,"Genshin 6.5","Genshin Impact","Hexenzirkel",8),
        (8,12,12,"Genshin 7.0","Genshin Impact","Anniversary",10),
        (2,14,14,"HSR 4.0","Honkai","Planarcadia",9),
        (4,26,30,"HSR 2nd Anniversary","Honkai","Major rewards",9),
        (12,1,4,"Spotify Wrapped","Spotify","Viral",10),
        (11,12,12,"Disney+ Day","Disney Plus","Annual",8),
        (5,22,22,"Mandalorian & Grogu","Disney Plus","Star Wars film",9),
        (11,26,26,"Stranger Things S5","Netflix","Final season",9),
        (12,18,18,"Avengers: Doomsday","Gift Cards","Biggest MCU",10),
        (2,17,17,"Chinese New Year","Gift Cards","Fire Horse",9),
        (7,7,10,"Amazon Prime Day","Gift Cards","Confirmed",9),
        (11,11,11,"Singles Day","Gift Cards","World's largest",9),
        (11,27,27,"Black Friday","Gift Cards","Peak sales",10),
        (11,30,30,"Cyber Monday","Gift Cards","Digital focus",9),
        (2,1,14,"Valentine's Day","Gift Cards","Gift peak",8),
        (5,1,12,"Mother's Day","Gift Cards","Major gift event",8),
        (6,1,15,"Father's Day","Gift Cards","Gaming gifting",8),
        (12,1,24,"Christmas Season","Gift Cards","Peak buying",10),
        (1,1,15,"Fortnite Winterfest","Fortnite","Holiday event",8),
        (10,15,31,"Fortnitemares","Fortnite","Halloween",8),
        (1,1,7,"Winter Anime","Crunchyroll","New premieres",7),
        (4,1,7,"Spring Anime","Crunchyroll","New premieres",7),
        (7,1,7,"Summer Anime","Crunchyroll","New premieres",7),
        (10,1,7,"Fall Anime","Crunchyroll","New premieres",7),
        (2,1,1,"Grammy Awards","Spotify","Music night",9),
        (3,15,15,"Oscars","Netflix","Film night",9),
        (3,21,21,"Monster Hunter Wilds","Game Release","Capcom PC/PS5/XSX",10),
        (2,28,28,"Elden Ring Nightreign","Game Release","FromSoftware",9),
    ]
    events = []
    for mo,d1,d2,name,cat,desc,pri in cal:
        try:
            s = datetime(today.year,mo,d1); e = datetime(today.year,mo,d2)
            if s < today-timedelta(days=30): s = datetime(today.year+1,mo,d1); e = datetime(today.year+1,mo,d2)
            days = (s-today).days
            if -10<=days<=60:
                if days<=0 and (e-today).days>=0: st,urg = "ACTIVE NOW","critical"
                elif days<=7: st,urg = f"{days}d","high"
                elif days<=14: st,urg = f"{days}d","medium"
                else: st,urg = f"{days}d","low"
                events.append({"name":name,"category":cat,"description":desc,"status":st,"urgency":urg,"days_until":days,"priority":pri,"is_live":False})
        except: continue
    seen = set()
    for q in ["game announcement today","release date announced","new update live","free games announced",
              "PlayStation Plus reveal","Game Pass announced","Nintendo Direct date","Steam sale date",
              "EA FC promo","Genshin banner","anime premiere","Crunchyroll new"]:
        try:
            feed = feedparser.parse(f"https://news.google.com/rss/search?q={quote(q)}+when:3d&hl=en-US&gl=US&ceid=US:en")
            for e in feed.entries[:3]:
                t = e.get("title",""); src = "News"
                if " - " in t: t,src = t.rsplit(" - ",1)
                k = t[:40].lower()
                if k in seen: continue
                seen.add(k); lo = t.lower()
                if any(x in lo for x in ["announce","reveal","launch","release","live now","available now","sale","event","promo","premiere"]):
                    cc = cats(t); cat2 = cc[0] if cc else "Gaming"
                    if any(x in lo for x in ["live now","out now","available now","today"]): urg,st = "critical","LIVE NOW"
                    elif any(x in lo for x in ["tomorrow","coming soon","this week"]): urg,st = "high","SOON"
                    else: urg,st = "medium","ANNOUNCED"
                    events.append({"name":t[:80],"category":cat2,"description":f"via {src}","status":st,"urgency":urg,
                                   "days_until":0 if urg=="critical" else 1,"priority":9 if urg=="critical" else 7,"is_live":True})
            time.sleep(0.2)
        except: continue
    for svc in ["Netflix","Prime Video","Disney Plus","Crunchyroll"]:
        try:
            feed = feedparser.parse(f"https://news.google.com/rss/search?q={quote(svc)}+new+release+when:7d&hl=en-US&gl=US&ceid=US:en")
            for e in feed.entries[:5]:
                t = e.get("title","")
                if " - " in t: t = t.rsplit(" - ",1)[0]
                if any(k in t.lower() for k in ["premieres","launches","releases","arrives","streaming","drops"]):
                    events.append({"name":f"{svc}: {t[:50]}","category":svc,"description":"Streaming release","status":"NOW","urgency":"high","days_until":0,"priority":7,"is_live":True})
            time.sleep(0.1)
        except: continue
    return sorted(events,key=lambda x:(-x["priority"] if x["urgency"]=="critical" else 0,x.get("days_until",99)))

# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = time.time()
    print("="*60); print("  RECHARGE.COM OPPORTUNITY SCANNER v4.2"); print(f"  {DATE} {TIME}"); print("="*60)
    sys.stdout.flush()
    events = get_events(); print(f"Events: {len(events)}"); sys.stdout.flush()
    all_sig = fetch_all()
    cands = comp_score(dedup(all_sig))
    ai = run_ai(cands, events, all_sig.get("competitor",[])); sys.stdout.flush()
    sheets_url = write_sheets(cands, ai, events); sys.stdout.flush()

    print("\n[STEP] Building HTML..."); sys.stdout.flush()
    try:
        html_file = build_html(cands, ai, events, all_sig)
        print(f"[STEP] HTML done: {html_file}"); sys.stdout.flush()
    except Exception as e:
        print(f"[ERROR] build_html failed: {type(e).__name__}: {e}"); sys.stdout.flush()
        import traceback; traceback.print_exc(); sys.stdout.flush()
        html_file = "index.html"
        with open(html_file,"w") as f: f.write("<h1>Scanner ran but HTML build failed</h1>")

    print("[STEP] Building Word doc..."); sys.stdout.flush()
    try:
        docx_file = build_docx(cands, ai, events, all_sig)
        print(f"[STEP] Word done: {docx_file}"); sys.stdout.flush()
    except Exception as e:
        print(f"[ERROR] build_docx failed: {type(e).__name__}: {e}"); sys.stdout.flush()
        docx_file = "none"

    print("[STEP] Building email..."); sys.stdout.flush()
    try:
        email_html = build_email_html(cands, ai, events, all_sig, DASHBOARD_URL)
        email_file = f"recharge_email_{DATE}.html"
        with open(email_file,"w",encoding="utf-8") as f: f.write(email_html)
        print(f"[STEP] Email HTML done: {email_file}"); sys.stdout.flush()
    except Exception as e:
        print(f"[ERROR] build_email_html failed: {type(e).__name__}: {e}"); sys.stdout.flush()
        email_html = ""; email_file = "none"

    print("[STEP] Sending email..."); sys.stdout.flush()
    try:
        send_email(email_html)
    except Exception as e:
        print(f"[ERROR] send_email failed: {type(e).__name__}: {e}"); sys.stdout.flush()

    elapsed = time.time()-t0; ex = ai.get("executive",{}); n_sources = len([k for k,v in all_sig.items() if len(v)>0])
    print("\n" + "="*60)
    print(f"  DONE in {elapsed:.0f}s | {n_sources} sources active")
    print(f"  HTML:  {html_file}")
    print(f"  Email: {email_file}")
    print(f"  Word:  {docx_file}")
    if sheets_url: print(f"  Sheet: {sheets_url}")
    print("="*60)
    print(f"\n{ex.get('summary','')}")
    print("\nActions:")
    for i,a in enumerate(ex.get("actions",[]),1): print(f"  {i}. {a}")
    sys.stdout.flush()
    return html_file, docx_file

if __name__ == "__main__":
    try:
        html_f, docx_f = main()
    except Exception as e:
        print(f"\n[FATAL] {type(e).__name__}: {e}"); sys.stdout.flush()
        import traceback; traceback.print_exc()
        sys.exit(1)
    if IS_COLAB:
        try:
            from google.colab import files; files.download(html_f); files.download(docx_f)
        except ImportError: pass
