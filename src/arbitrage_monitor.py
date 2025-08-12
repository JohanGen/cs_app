"""
CS Odds Arbitrage Monitor — Headless (Playwright) edition

- Polls GG.BET, Thunderpick, and Beonbet every 10s (headless Chromium)
- Extracts 2-way match odds (Team1/Team2) using robust, multi-selector scraping
- Matches identical matches across books with fuzzy/alias matching
- Detects 2-way arbitrage and prints stake split for a given bankroll
- Logs found opportunities to CSV

Setup
-----
pip install requests beautifulsoup4 python-dateutil rapidfuzz pydantic playwright
playwright install chromium

Run
---
python arbitrage_monitor.py

Edit the selectors in PRESETS below if a site changes its HTML.
"""
from __future__ import annotations

import time
import csv
import math
import random
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup  # type: ignore
from rapidfuzz import fuzz, process  # type: ignore
from dateutil import parser as dtparser  # type: ignore

# ------------------------------
# Config
# ------------------------------
POLL_INTERVAL_SECONDS = 10
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 12
CSV_LOG_PATH = "arbitrage_log.csv"
FUZZY_MATCH_THRESHOLD = 90

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ------------------------------
# Data Models
# ------------------------------
@dataclass
class Selection:
    name: str
    odds: float

@dataclass
class EventOdds:
    match_id: str
    site: str
    sport: str
    league: Optional[str]
    start_time: Optional[datetime]
    team1: str
    team2: str
    odds_team1: float
    odds_team2: float
    raw: dict = field(default_factory=dict)

    def teams_key(self) -> Tuple[str, str]:
        t1 = normalize_team(self.team1)
        t2 = normalize_team(self.team2)
        return tuple(sorted([t1, t2]))  # type: ignore

# ------------------------------
# Helpers
# ------------------------------
ALIASES: Dict[str, List[str]] = {
    # Fill if you hit nickname collisions across sites
    # "ninjas in pyjamas": ["nip", "ninjas in pajamas"],
}

def normalize_team(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("esports", "").replace("esport", "").replace("team", "").strip()
    return s

def alias_lookup(name: str) -> str:
    norm = normalize_team(name)
    best = norm
    for canonical, alt_list in ALIASES.items():
        choices = [canonical] + alt_list
        match = process.extractOne(norm, choices, scorer=fuzz.token_set_ratio)  # type: ignore
        if match and match[1] >= FUZZY_MATCH_THRESHOLD:
            best = canonical
            break
    return best

def parse_decimal(txt: str) -> float:
    s = txt.strip().replace(" ", " ")
    keep = ''.join(ch for ch in s if (ch.isdigit() or ch in '.,'))
    if keep.count(',') == 1 and keep.count('.') == 0:
        keep = keep.replace(',', '.')
    parts = keep.split('.')
    if len(parts) > 2:
        keep = parts[0] + '.' + ''.join(parts[1:])
    return float(keep)

def implied_prob(odds: float) -> float:
    return 1.0 / odds

# ------------------------------
# Adapters
# ------------------------------
class BaseAdapter:
    site_name: str = "base"

    def fetch(self) -> List[EventOdds]:
        raise NotImplementedError

    @staticmethod
    def _session() -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        return s

# Playwright adapter (headless browser)
try:
    from playwright.sync_api import sync_playwright  # type: ignore
    HAVE_PLAYWRIGHT = True
except Exception:
    HAVE_PLAYWRIGHT = False

class PlaywrightHTMLAdapter(BaseAdapter):
    """Use a headless browser for JS-rendered odds pages.
    - selectors: dict with keys 'containers' (list[str]), 'team1', 'team2', 'odds1', 'odds2', optional 'start'
    Each of 'team*'/'odds*' can be a list[str] of alternative selectors. We'll try in order.
    """
    site_name = "JSRenderedSite"

    def __init__(self, url: str, site_name: str, selectors: dict, wait_selector: Optional[str] = None):
        self.url = url
        self.site_name = site_name
        self.selectors = selectors
        self.wait_selector = wait_selector

    def _first_text(self, node: BeautifulSoup, sels: List[str]) -> Optional[str]:
        for sel in sels:
            el = node.select_one(sel)
            if el:
                txt = el.get_text(strip=True)
                if txt:
                    return txt
        return None

    def _extract(self, html) -> List[EventOdds]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[EventOdds] = []
        containers = []
        for csel in self.selectors["containers"]:
            containers = soup.select(csel)
            if containers:
                break
        if not containers:
            return results

        team1_sels = self.selectors.get("team1", [])
        team2_sels = self.selectors.get("team2", [])
        odds1_sels = self.selectors.get("odds1", [])
        odds2_sels = self.selectors.get("odds2", [])
        start_sels = self.selectors.get("start", []) or []

        for match in containers:
            try:
                t1 = self._first_text(match, team1_sels)
                t2 = self._first_text(match, team2_sels)
                o1_txt = self._first_text(match, odds1_sels)
                o2_txt = self._first_text(match, odds2_sels)
                if not (t1 and t2 and o1_txt and o2_txt):
                    continue
                o1 = parse_decimal(o1_txt)
                o2 = parse_decimal(o2_txt)
                start = None
                if start_sels:
                    st_txt = self._first_text(match, start_sels)
                    if st_txt:
                        try:
                            start = dtparser.parse(st_txt)
                        except Exception:
                            start = None
                results.append(EventOdds(
                    match_id=f"pw-{t1}-{t2}",
                    site=self.site_name,
                    sport="CS2",
                    league=None,
                    start_time=start,
                    team1=t1,
                    team2=t2,
                    odds_team1=o1,
                    odds_team2=o2,
                ))
            except Exception:
                continue
        return results

    def fetch(self) -> List[EventOdds]:
        if not HAVE_PLAYWRIGHT:
            logging.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(user_agent=USER_AGENT)
                page = ctx.new_page()
                page.goto(self.url, wait_until="domcontentloaded")
                # wait on any of the container selectors
                waited = False
                for csel in self.selectors["containers"]:
                    try:
                        page.wait_for_selector(csel, timeout=8000)
                        waited = True
                        break
                    except Exception:
                        continue
                if not waited:
                    page.wait_for_timeout(2000)
                html = page.content()
                ctx.close(); browser.close()
                return self._extract(html)
        except Exception as e:
            logging.warning(f"{self.site_name}: playwright fetch error: {e}")
            return []

# ------------------------------
# Matching & Arbitrage
# ------------------------------

def group_by_match(events: List[EventOdds]) -> Dict[Tuple[str, str], List[EventOdds]]:
    buckets: Dict[Tuple[str, str], List[EventOdds]] = {}
    for ev in events:
        k = tuple(sorted([alias_lookup(ev.team1), alias_lookup(ev.team2)]))  # type: ignore
        buckets.setdefault(k, []).append(ev)
    return buckets


def detect_two_way_arbitrage(booklines: List[EventOdds]) -> Optional[dict]:
    if not booklines:
        return None
    best_t1 = max(booklines, key=lambda e: e.odds_team1)
    best_t2 = max(booklines, key=lambda e: e.odds_team2)
    p_sum = implied_prob(best_t1.odds_team1) + implied_prob(best_t2.odds_team2)
    if p_sum < 1.0:
        margin = 1.0 - p_sum
        return {
            "team1": best_t1.team1,
            "team2": best_t2.team2,
            "site_team1": best_t1.site,
            "site_team2": best_t2.site,
            "odds_team1": best_t1.odds_team1,
            "odds_team2": best_t2.odds_team2,
            "prob_sum": p_sum,
            "margin": margin,
        }
    return None


def stake_split(total_stake: float, odds1: float, odds2: float) -> Tuple[float, float, float]:
    inv1 = 1.0 / odds1
    inv2 = 1.0 / odds2
    denom = inv1 + inv2
    s1 = total_stake * (inv1 / denom)
    s2 = total_stake - s1
    profit = min(s1 * (odds1 - 1.0), s2 * (odds2 - 1.0))
    return (round(s1, 2), round(s2, 2), round(profit, 2))

# ------------------------------
# Site presets (selectors)
# ------------------------------
# These lists include multiple fallback selectors — the adapter tries them in order.

PRESETS = [
    {
        "site": "GG.BET",
        "url": "https://gg.bet/?sportId=esports_counter_strike",
        "selectors": {
            "containers": [
                "[data-test='events-list'] .event-row",
                ".c-events__item",
                "[class*='events'] [class*='row']",
            ],
            "team1": [
                ".participant:first-child",
                ".c-events__team--home",
                "[class*='team'] [class*='name']:first-child",
            ],
            "team2": [
                ".participant:last-child",
                ".c-events__team--away",
                "[class*='team'] [class*='name']:last-child",
            ],
            "odds1": [
                ".coef:first-child",
                ".c-bets .c-bet:nth-child(1)",
                "[class*='odds'] :nth-child(1)",
            ],
            "odds2": [
                ".coef:last-child",
                ".c-bets .c-bet:nth-child(2)",
                "[class*='odds'] :nth-child(2)",
            ],
        },
    },
    {
        "site": "Thunderpick",
        "url": "https://thunderpick.io/esports/cs2-betting",
        "selectors": {
            "containers": [
                "[data-testid='events-list'] [data-testid='event-row']",
                "[data-testid*='event']",
                ".match-card, [class*='MatchCard']",
            ],
            "team1": [
                "[data-testid='team-a-name']",
                ".team-name:first-child",
                "[class*='Team'] [class*='name']:first-child",
            ],
            "team2": [
                "[data-testid='team-b-name']",
                ".team-name:last-child",
                "[class*='Team'] [class*='name']:last-child",
            ],
            "odds1": [
                "[data-testid='odd-a']",
                ".odds-value:nth-child(1)",
                "[class*='odds'] :nth-child(1)",
            ],
            "odds2": [
                "[data-testid='odd-b']",
                ".odds-value:nth-child(2)",
                "[class*='odds'] :nth-child(2)",
            ],
        },
    },
    {
        "site": "Beonbet",
        "url": "https://beonbet.com/no/sport?bt-path=/counter-strike/counter-strike-2/b--starladder-ss-fall-2025-eu-qualifier-2566766243860324375",
        "selectors": {
            "containers": [
                "[data-test*='event'], [data-testid*='event']",
                ".event, .line-event, .match, [class*='EventCard']",
            ],
            "team1": [
                ".team-name:first-child",
                "[class*='team'] [class*='name']:first-child",
                "[data-test*='team']:first-child",
            ],
            "team2": [
                ".team-name:last-child",
                "[class*='team'] [class*='name']:last-child",
                "[data-test*='team']:last-child",
            ],
            "odds1": [
                ".coeff, .coef, .odd, [data-test*='odd']:nth-child(1)",
                "[class*='bet'] [class*='coeff']:nth-child(1)",
            ],
            "odds2": [
                ".coeff:last-child, .coef:last-child, .odd:last-child, [data-test*='odd']:nth-child(2)",
                "[class*='bet'] [class*='coeff']:nth-child(2)",
            ],
        },
    },
]

# ------------------------------
# CSV Logging
# ------------------------------

def ensure_csv_header(path: str):
    try:
        with open(path, "x", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "ts","team1","team2","site_team1","site_team2","odds1","odds2","prob_sum","margin","stake1","stake2","profit"
            ])
    except FileExistsError:
        pass


def log_opportunity(path: str, row: List):
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ------------------------------
# Main Loop
# ------------------------------

def presets_to_adapters() -> List[BaseAdapter]:
    adapters: List[BaseAdapter] = []
    for cfg in PRESETS:
        adapters.append(PlaywrightHTMLAdapter(cfg["url"], cfg["site"], cfg["selectors"]))
    return adapters

def run(total_stake: float = 400.0):
    adapters: List[BaseAdapter] = presets_to_adapters()
    ensure_csv_header(CSV_LOG_PATH)

    while True:
        start_ts = time.time()
        all_events: List[EventOdds] = []
        for ad in adapters:
            events = ad.fetch()
            logging.info(f"Fetched {len(events)} events from {ad.site_name}")
            all_events.extend(events)

        by_match = group_by_match(all_events)
        found = 0
        for key, booklines in by_match.items():
            if len(booklines) < 2:
                continue
            arb = detect_two_way_arbitrage(booklines)
            if arb:
                s1, s2, profit = stake_split(total_stake, arb["odds_team1"], arb["odds_team2"])
                found += 1
                t1, t2 = key
                logging.warning(
                    (
                        f"ARB: {t1} vs {t2} | {arb['site_team1']} @ {arb['odds_team1']} / "
                        f"{arb['site_team2']} @ {arb['odds_team2']} | margin={arb['margin']*100:.2f}% | "
                        f"stakes: {s1}/{s2} | profit≈{profit}"
                    )
                )
                log_opportunity(
                    CSV_LOG_PATH,
                    [
                        datetime.now(timezone.utc).isoformat(),
                        t1,
                        t2,
                        arb["site_team1"],
                        arb["site_team2"],
                        arb["odds_team1"],
                        arb["odds_team2"],
                        round(arb["prob_sum"], 5),
                        round(arb["margin"], 5),
                        s1,
                        s2,
                        profit,
                    ],
                )
        if found == 0:
            logging.info("No arbitrage this cycle.")

        elapsed = time.time() - start_ts
        sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for + random.random() * 0.5)

if __name__ == "__main__":
    run(total_stake=400.0)
