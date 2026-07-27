#!/usr/bin/env python3
"""Scraper Anime-Sama (anime-sama.to) avec cloudscraper.

V2.15 — Stable anime_id via hash MD5 de l'URL.
  - anime_id = int.from_bytes(md5(url).digest()[:4], 'big')
  - Avant : compteur séquentiel (next_anime_id) → IDs instables entre runs
  - Maintenant : même URL → même anime_id à JAMAIS, peu importe l'ordre de scrap
  - Les données user (continue_watching, favoris, downloads, progress) restent
    valides à travers tous les refresh DB
  - La table `discover` n'est plus remplie par ce script — l'app la hardcode
    et résout par titre au runtime (voir AnimeDao.getDiscover)

Compatibilité avec DB existante :
  - Les anciens anime_id (1, 2, 3...) ne matcheront plus les nouveaux.
  - C'est attendu : les données user_data.db anciennes deviennent invalides.
  - L'utilisateur peut les clearer manuellement (croix sur "Continue Watching",
    bouton "Supprimer tous les favoris" dans Settings si dispo).
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
import unicodedata
from ast import literal_eval
from html import unescape
from typing import Any
from urllib.parse import urlparse

import cloudscraper
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from huggingface_hub import HfApi, hf_hub_download, whoami
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("cloudscraper").setLevel(logging.WARNING)
log = logging.getLogger("scrap")

REQUEST_DELAY = 0.3

SCHEMA = """
CREATE TABLE anime (
    anime_id             INTEGER PRIMARY KEY,
    title                TEXT NOT NULL,
    title_normalized     TEXT NOT NULL,
    original_title       TEXT,
    alternative_titles   TEXT,
    description          TEXT,
    image                TEXT,
    image_url            TEXT,
    year                 INTEGER,
    status               TEXT,
    rating               REAL,
    featured             INTEGER DEFAULT 0,
    has_episodes         INTEGER DEFAULT 0,
    seasons_fetched      INTEGER DEFAULT 0,
    languages            TEXT,
    raw_json             TEXT NOT NULL
);
CREATE TABLE genre (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT UNIQUE NOT NULL,
    name_normalized  TEXT UNIQUE NOT NULL
);
CREATE TABLE anime_genre (
    anime_id  INTEGER NOT NULL,
    genre_id  INTEGER NOT NULL,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES anime(anime_id),
    FOREIGN KEY (genre_id) REFERENCES genre(id)
);
CREATE TABLE season (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id      INTEGER NOT NULL,
    season_number INTEGER NOT NULL,
    name          TEXT,
    UNIQUE (anime_id, season_number, name),
    FOREIGN KEY (anime_id) REFERENCES anime(anime_id)
);
CREATE TABLE episode (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id      INTEGER NOT NULL,
    episode_number INTEGER NOT NULL,
    title          TEXT,
    description    TEXT,
    duration       TEXT,
    languages      TEXT,
    UNIQUE (season_id, episode_number),
    FOREIGN KEY (season_id) REFERENCES season(id)
);
CREATE TABLE episode_url (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id   INTEGER NOT NULL,
    language     TEXT NOT NULL,
    url          TEXT NOT NULL,
    url_position INTEGER NOT NULL,
    host         TEXT NOT NULL,
    FOREIGN KEY (episode_id) REFERENCES episode(id)
);
CREATE TABLE discover (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    position     INTEGER NOT NULL,
    anime_id     INTEGER,
    title        TEXT,
    description  TEXT,
    image        TEXT,
    rating       REAL,
    has_episodes INTEGER DEFAULT 0,
    raw_json     TEXT NOT NULL
);
CREATE INDEX idx_anime_title_norm    ON anime(title_normalized);
CREATE INDEX idx_anime_year          ON anime(year);
CREATE INDEX idx_anime_rating        ON anime(rating);
CREATE INDEX idx_anime_has_episodes  ON anime(has_episodes);
CREATE INDEX idx_genre_name_norm     ON genre(name_normalized);
CREATE INDEX idx_anime_genre_genre   ON anime_genre(genre_id);
CREATE INDEX idx_anime_genre_anime   ON anime_genre(anime_id);
CREATE INDEX idx_season_anime        ON season(anime_id);
CREATE INDEX idx_episode_season      ON episode(season_id);
CREATE INDEX idx_episode_url_ep      ON episode_url(episode_id);
CREATE INDEX idx_episode_url_host    ON episode_url(host);
CREATE INDEX idx_episode_url_lang    ON episode_url(episode_id, language);
"""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


# ==================================================================
# V2.15 — Stable anime_id via hash MD5 de l'URL
# V2.15.4 — Masque 0x7FFFFFFF pour garantir un ID dans la plage Int signé
#           Java/Kotlin (max 2^31 - 1 = 2 147 483 647). Avant ce masque,
#           ~50% des anime_id dépassaient Int.MAX_VALUE et causaient un
#           overflow silencieux côté app → "Anime non trouvé".
# ==================================================================
def stable_anime_id(url: str) -> int:
    """
    Calcule un anime_id déterministe à partir de l'URL source.

    - 4 premiers octets du MD5 de l'URL normalisée (sans trailing slash)
    - V2.15.4 : masque `& 0x7FFFFFFF` → 31 bits, IDs entre 0 et 2^31-1
      (tient dans un Int signé Java/Kotlin, évite l'overflow côté app)
    - Même URL → même ID, peu importe l'ordre de scrap ou l'état du state
    - Collisions : négligeables avec 31 bits pour ~1300 animes
      (proba ~ 1e-7 d'avoir une collision avec 1300 entrées sur 2e9 espace)
    """
    if not url:
        return 0
    # Normaliser l'URL : strip trailing slash, lowercase le host
    norm_url = url.rstrip("/").strip()
    try:
        parsed = urlparse(norm_url)
        if parsed.netloc:
            norm_url = f"{parsed.scheme}://{parsed.netloc.lower()}{parsed.path}"
            if parsed.query:
                norm_url += f"?{parsed.query}"
    except Exception:
        pass

    digest = hashlib.md5(norm_url.encode("utf-8")).digest()
    # V2.15.4 : & 0x7FFFFFFF → garantit un ID positif dans la plage Int Java
    anime_id = int.from_bytes(digest[:4], "big") & 0x7FFFFFFF
    # Éviter l'ID 0 (réservé pour "non trouvé")
    if anime_id == 0:
        anime_id = 1
    return anime_id


def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def extract_host(url: str) -> str:
    if not url:
        return "unknown"
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return "unknown"
    if "vidmoly" in host:        return "vidmoly"
    if "sendvid" in host:        return "sendvid"
    if "sibnet" in host:         return "sibnet"
    if "vk.com" in host:         return "vk"
    if "doodstream" in host or "dood." in host: return "doodstream"
    if "streamtape" in host:     return "streamtape"
    if "streamwish" in host or "streamz" in host: return "streamwish"
    if "smoothpre" in host or "vidhide" in host: return "streamwish"
    if "mega.nz" in host or "mega.co.nz" in host: return "mega"
    if "youtube" in host:        return "youtube"
    if "tune" in host or "hydrax" in host: return "hydrax"
    if "uqload" in host:         return "uqload"
    return host or "unknown"


def fix_image_url(url: str) -> str:
    if not url:
        return url
    return url.replace(
        "cdn.statically.io/gh/Anime-Sama/IMG/img",
        "raw.githubusercontent.com/Anime-Sama/IMG/img",
    )


def remove_some_js_comments(string: str) -> str:
    string = re.sub(r"\/\*[\W\w]*?\*\/", "", string)
    return re.sub(r"<!--[\W\w]*?-->", "", string)


def split_and_strip(string: str, delimiters) -> list[str]:
    if isinstance(delimiters, str):
        return [part.strip() for part in string.split(delimiters)]
    string_list = [string]
    for delimiter in delimiters:
        string_list = sum((part.split(delimiter) for part in string_list), [])
    return [part.strip() for part in string_list]


class ScraperClient:
    """Pool de sessions cloudscraper pour bypass Cloudflare + parallélisme."""

    POOL_SIZE = 4

    def __init__(self):
        self._sessions = [
            cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            for _ in range(self.POOL_SIZE)
        ]
        self._session_idx = 0
        self._session_lock = asyncio.Lock()
        self.req_count = 0
        self._concurrency = asyncio.Semaphore(self.POOL_SIZE)

    async def _get_session(self):
        async with self._session_lock:
            session = self._sessions[self._session_idx]
            self._session_idx = (self._session_idx + 1) % self.POOL_SIZE
            return session

    async def get(self, url: str, *, retry: int = 3) -> str:
        async with self._concurrency:
            for attempt in range(retry):
                await asyncio.sleep(REQUEST_DELAY)
                try:
                    session = await self._get_session()
                    resp = await asyncio.to_thread(
                        session.get,
                        url,
                        headers={
                            "User-Agent": USER_AGENT,
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
                        },
                        timeout=30,
                    )
                    self.req_count += 1
                    if resp.status_code in (429, 500, 502, 503, 504):
                        wait = 2 ** attempt + random.uniform(0, 1)
                        log.warning("HTTP %d sur %s — retry dans %.1fs", resp.status_code, url, wait)
                        await asyncio.sleep(wait)
                        continue
                    title_match = re.search(r"<title>([^<]+)</title>", resp.text, re.IGNORECASE)
                    title = title_match.group(1).lower() if title_match else ""
                    if "blocked" in title or "attention required" in title:
                        wait = 5 + attempt * 5
                        log.warning("Cloudflare block sur %s — retry dans %ds", url, wait)
                        await asyncio.sleep(wait)
                        continue
                    text = resp.text
                    if ".js" in url:
                        if len(text) < 30 and "eps" not in text:
                            await asyncio.sleep(2)
                            continue
                    else:
                        if len(text) < 100 and "Page introuvable" not in text:
                            await asyncio.sleep(2)
                            continue
                    return text
                except Exception as e:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    log.warning("Erreur réseau sur %s: %s — retry dans %.1fs", url, e, wait)
                    await asyncio.sleep(wait)
            log.error("Échec définitif après %d retries : %s", retry, url)
            return ""

    def close(self):
        for session in self._sessions:
            try:
                session.close()
            except Exception:
                pass


async def find_site_url(client: ScraperClient) -> str | None:
    log.info("Recherche du domaine actif via anime-sama.org ...")
    test_url = "https://anime-sama.org/catalogue/?search="
    html = await client.get(test_url)
    if html and html.count('class="card-title"') > 0:
        try:
            final_url = await asyncio.to_thread(
                lambda: client._sessions[0].get(test_url, allow_redirects=True).url
            )
            parsed = urlparse(str(final_url))
            site_url = f"{parsed.scheme}://{parsed.netloc}/"
            log.info("  ✓ Domaine actif : %s", site_url)
            return site_url
        except Exception:
            return "https://anime-sama.org/"

    log.warning("anime-sama.org inaccessible — fallback liste codée en dur")
    for url in [
        "https://anime-sama.to/",
        "https://anime-sama.si/",
        "https://anime-sama.tv/",
        "https://anime-sama.eu/",
        "https://anime-sama.org/",
    ]:
        log.info("Test domaine %s ...", url)
        html_test = await client.get(url + "catalogue/?search=")
        if html_test and html_test.count('class="card-title"') > 0:
            log.info("  ✓ Domaine actif : %s", url)
            return url
    log.error("Aucun domaine Anime-Sama accessible")
    return None


def parse_catalogue_page(html: str, site_url: str) -> list[dict]:
    results = []
    soup = BeautifulSoup(html, "lxml")
    flag_to_lang = {"JP": "VOSTFR", "FR": "VF", "EN": "VASTFR", "CN": "VCN", "KR": "VKR", "QC": "VQC"}
    flag_png_to_lang = {"jp": "VOSTFR", "fr": "VF", "en": "VASTFR", "cn": "VCN", "kr": "VKR", "qc": "VQC"}

    cards = soup.find_all("div", class_="shrink-0 catalog-card card-base")
    for card in cards:
        link_tag = card.find("a", href=True)
        if not link_tag:
            continue
        url = link_tag["href"]
        if url.startswith("/"):
            url = site_url.rstrip("/") + url
        if not url.endswith("/"):
            url += "/"

        img_tag = card.find("img")
        image_url = img_tag.get("src", "") if img_tag else ""
        name = ""
        title_tag = card.find("h2", class_="card-title")
        if title_tag:
            name = title_tag.get_text(strip=True)
        elif img_tag:
            name = img_tag.get("alt", "").strip()

        alt_tag = card.find("p", class_="alternate-titles")
        alt_names_str = alt_tag.get_text(strip=True) if alt_tag else ""
        alternative_names = [n.strip() for n in alt_names_str.split(",") if n.strip()] if alt_names_str else []

        genres = []
        for g_tag in card.find_all("span", class_="genre-tag"):
            g = g_tag.get_text(strip=True)
            if g and g != "…":
                genres.append(g)

        categories_str = ""
        info_values = card.find_all("p", class_="info-value")
        if info_values:
            categories_str = info_values[0].get_text(strip=True)
        categories_clean = set()
        for cat in [c.strip() for c in categories_str.split(",") if c.strip()]:
            cl = cat.lower()
            if cl.startswith("anime"):
                categories_clean.add("Anime")
            elif cl.startswith("scan"):
                categories_clean.add("Scans")
            elif cl in ("film", "films"):
                categories_clean.add("Film")
            elif cl.startswith("autre"):
                categories_clean.add("Autres")

        languages = set()
        has_jp = False
        has_fr = False
        for flag_tag in card.find_all("span", class_="lang-flag"):
            flag = (flag_tag.get("title") or "").strip().upper()
            if flag == "JP":
                has_jp = True
            elif flag == "FR":
                has_fr = True
            elif flag in flag_to_lang:
                languages.add(flag_to_lang[flag])
        if not has_jp and not has_fr and not languages:
            for img in card.find_all("img", class_="flag-icon"):
                src = img.get("src", "")
                flag_match = re.search(r"flag_([a-z]+)\.png", src)
                if flag_match:
                    flag = flag_match.group(1).lower()
                    if flag == "jp":
                        has_jp = True
                    elif flag == "fr":
                        has_fr = True
                    elif flag in flag_png_to_lang:
                        languages.add(flag_png_to_lang[flag])
        if has_fr:
            languages.add("VF")
        if has_jp:
            if has_fr:
                languages.add("VOSTFR")
            else:
                languages.add("VJSTFR")

        synopsis = ""
        syn_tag = card.find("div", class_="synopsis-content")
        if syn_tag:
            synopsis = syn_tag.get_text(strip=True)

        results.append({
            "url": url,
            "image_url": image_url,
            "name": name,
            "alternative_names": alternative_names,
            "genres": genres,
            "categories": categories_clean,
            "languages": languages,
            "synopsis": synopsis,
        })
    return results


async def fetch_all_catalogues(
    client: ScraperClient, site_url: str, max_animes: int | None = None
) -> list[dict]:
    log.info("Récupération du catalogue complet depuis %scatalogue/ ...", site_url)
    all_catalogues: list[dict] = []
    seen_urls: set[str] = set()
    page = 1
    scans_filtered = 0

    while True:
        if max_animes and len(all_catalogues) >= max_animes:
            break
        html = await client.get(f"{site_url}catalogue/?page={page}")
        if not html:
            log.error("Page %d : erreur — arrêt", page)
            break

        soup = BeautifulSoup(html, "lxml")
        empty_marker = soup.find("p", class_="text-white font-bold text-2xl h-96 p-5")
        if empty_marker:
            log.info("Page %d : page vide — fin du catalogue", page)
            break

        page_catalogues = parse_catalogue_page(html, site_url)
        if not page_catalogues:
            log.info("Page %d : 0 animes — fin", page)
            break

        new_count = 0
        for cat in page_catalogues:
            cats = cat.get("categories", set())
            if not (cats & {"Anime", "Film"}):
                scans_filtered += 1
                continue
            if cat["url"] not in seen_urls:
                seen_urls.add(cat["url"])
                all_catalogues.append(cat)
                new_count += 1

        log.info("Page %d : %d nouveaux (%d total, %d scans filtrés)", page, new_count, len(all_catalogues), scans_filtered)
        page += 1

    if max_animes:
        all_catalogues = all_catalogues[:max_animes]
    log.info("Catalogue : %d animes (%d scans filtrés)", len(all_catalogues), scans_filtered)
    return all_catalogues


LANG_IDS_TO_FETCH = ["vostfr", "vf", "vf1", "vf2", "va", "vcn", "vj", "vkr", "vqc", "vo", "var"]
LANG_ID_TO_NAME = {
    "vostfr": "VOSTFR", "vf": "VF", "vf1": "VF", "vf2": "VF",
    "va": "VASTFR", "vcn": "VCN", "vj": "VJSTFR", "vkr": "VKR",
    "vqc": "VQC", "vo": "VO", "var": "VAR",
}


async def fetch_anime_page(client: ScraperClient, url: str) -> str:
    return await client.get(url)


def parse_seasons_from_page(html: str, base_url: str) -> list[dict]:
    html_clean = remove_some_js_comments(html)
    soup = BeautifulSoup(html_clean, "lxml")
    seasons = []
    pattern = re.compile(
        r'panneau(?:Anime|Film)\s*\(\s*(["\'])(.*?)\1\s*,\s*(["\'])(.*?)\3\s*\)'
    )
    seen_links = set()
    for script in soup.find_all("script"):
        if not script.string:
            continue
        text = re.sub(r"/\*.*?\*/", "", script.string, flags=re.DOTALL)
        for quote1, nom, quote2, lien in pattern.findall(text):
            if nom.lower() == "nom" or lien.lower() == "url":
                continue
            lien_clean = re.sub(r"/?(?:vostfr|vf\d*|va|vcn|vj|vkr|vqc|vo|var)/?$", "", lien)
            if lien_clean in seen_links:
                continue
            seen_links.add(lien_clean)
            season_url = base_url.rstrip("/") + "/" + lien_clean.lstrip("/")
            if not season_url.endswith("/"):
                season_url += "/"
            seasons.append({"name": nom.strip(), "url": season_url})
    return seasons


async def fetch_season_lang_page(
    client: ScraperClient, season_url: str, lang_id: str
) -> tuple[str, str]:
    page_url = season_url + lang_id + "/"
    html = await client.get(page_url)
    if not html:
        return "", ""
    if "Page introuvable" in html or "Accès Introuvable" in html:
        return "", ""
    soup = BeautifulSoup(html, "lxml")
    script_tag = soup.find("script", src=lambda s: s and "episodes.js" in s)
    if not script_tag:
        match_url = re.search(r"episodes\.js\?filever=\d+", html)
        if not match_url:
            return html, ""
        js_url = page_url + match_url.group(0)
    else:
        js_url = page_url + script_tag["src"]
    js_html = await client.get(js_url)
    if not js_html:
        return html, ""
    return html, js_html


def parse_players_from_js(episodes_js: str) -> list[list[str]]:
    js_clean = remove_some_js_comments(episodes_js)
    matches = re.findall(r"(?:var\s+)?(eps\d+)\s*=\s*\[(.*?)\];", js_clean, re.DOTALL)
    if not matches:
        return []
    players_dict = {}
    for name, content in matches:
        player_num = int(re.search(r"\d+", name).group())
        urls = re.findall(r"'(https?://[^']+)'", content)
        urls = [u.replace("vidmoly.to", "vidmoly.net") for u in urls]
        players_dict[player_num] = urls
    if not players_dict:
        return []
    num_episodes = max(len(urls) for urls in players_dict.values())
    sorted_player_nums = sorted(players_dict.keys())
    episodes_out = []
    for ep_idx in range(num_episodes):
        episode_urls = []
        for player_num in sorted_player_nums:
            urls = players_dict[player_num]
            if ep_idx < len(urls) and urls[ep_idx]:
                episode_urls.append(urls[ep_idx])
        if episode_urls:
            episodes_out.append(episode_urls)
    return episodes_out


def parse_episode_names(html: str, num_episodes: int, num_max: int) -> list[str]:
    html_clean = remove_some_js_comments(html)
    functions = re.findall(r"resetListe\(\); *[\n\r]+\t*(.*?)}", html_clean, re.DOTALL)
    if not functions:
        return [f"Episode {n}" for n in range(1, num_episodes + 1)]
    functions_list = split_and_strip(functions[-1], (";", "\n"))[:-1]

    def padding(n: int) -> str:
        return " " * (len(str(num_max)) - len(str(n)))

    def episode_name_range(*args) -> list[str]:
        return [f"Episode {n}{padding(n)}" for n in range(*args)]

    episodes_name: list[str] = []
    for function in functions_list:
        if function.startswith("//"):
            continue
        call_start = function.find("(")
        if call_start == -1:
            continue
        fname = function[:call_start]
        args_str = function[call_start + 1 : -1]
        try:
            args = literal_eval(args_str + ",") if args_str else ()
        except Exception:
            continue
        if not isinstance(args, tuple):
            continue
        if fname == "creerListe":
            if len(args) < 2:
                continue
            episodes_name += episode_name_range(int(args[0]), int(args[1]) + 1)
        elif fname in ("finirListe", "finirListeOP"):
            if not args:
                break
            episodes_name += episode_name_range(int(args[0]), int(args[0]) + num_episodes - len(episodes_name))
            break
        elif fname == "newSP":
            if not args:
                continue
            episodes_name.append(f"Episode {args[0]}")
        elif fname == "newSPF":
            if not args:
                continue
            episodes_name.append(str(args[0]))
    return episodes_name


async def scrape_anime(
    client: ScraperClient, catalogue: dict, anime_id: int
) -> dict:
    url = catalogue["url"]
    name = catalogue["name"]

    page_html = await fetch_anime_page(client, url)

    synopsis = catalogue.get("synopsis", "")
    if not synopsis:
        syn_match = re.search(r"Synopsis[\W\w]+?>(.+)<", page_html)
        if syn_match:
            synopsis = unescape(syn_match.group(1)).strip()

    image_url = catalogue.get("image_url", "")
    if image_url:
        image_url = fix_image_url(image_url)

    seasons_raw = parse_seasons_from_page(page_html, url)

    seasons_out = []
    for season_idx, season_info in enumerate(seasons_raw, start=1):
        season_name = season_info["name"]
        season_url = season_info["url"]
        sn_match = re.search(r"\d+", season_name)
        season_number = int(sn_match.group(0)) if sn_match else season_idx
        if season_name.lower() in ("film", "films"):
            season_number = 99

        lang_episodes: dict[str, list[list[str]]] = {}
        lang_names: dict[str, list[str]] = {}

        lang_results = await asyncio.gather(
            *(fetch_season_lang_page(client, season_url, lid) for lid in LANG_IDS_TO_FETCH)
        )
        for lang_id, (html, js) in zip(LANG_IDS_TO_FETCH, lang_results):
            if not html and not js:
                continue
            players = parse_players_from_js(js) if js else []
            if not players:
                continue
            num_ep = len(players)
            names = parse_episode_names(html, num_ep, num_ep)
            while len(names) < num_ep:
                names.append(f"Episode {len(names) + 1}")
            lang_name = LANG_ID_TO_NAME.get(lang_id, lang_id.upper())
            if lang_name in lang_episodes:
                if len(players) > len(lang_episodes[lang_name]):
                    lang_episodes[lang_name] = players
                    lang_names[lang_name] = names
            else:
                lang_episodes[lang_name] = players
                lang_names[lang_name] = names

        if not lang_episodes:
            continue

        max_eps = max(len(eps) for eps in lang_episodes.values())
        episodes_out = []
        for ep_idx in range(max_eps):
            ep_num = ep_idx + 1
            ep_name = f"Episode {ep_num}"
            for lang in ["VOSTFR", "VF", "VJSTFR", "VASTFR", "VCN", "VKR", "VQC"]:
                if lang in lang_names and ep_idx < len(lang_names[lang]):
                    ep_name = lang_names[lang][ep_idx]
                    break
            ep_urls: dict[str, list[dict]] = {}
            ep_langs = []
            for lang, eps_list in lang_episodes.items():
                if ep_idx < len(eps_list) and eps_list[ep_idx]:
                    ep_urls[lang] = [{"host": extract_host(u), "url": u} for u in eps_list[ep_idx]]
                    ep_langs.append(lang)
            if ep_urls:
                episodes_out.append({
                    "episode_number": ep_num,
                    "title": ep_name,
                    "languages": ep_langs,
                    "urls": ep_urls,
                })

        if episodes_out:
            seasons_out.append({
                "season_number": season_number,
                "name": season_name,
                "episodes": episodes_out,
            })

    real_languages = set()
    for season in seasons_out:
        for episode in season.get("episodes", []):
            real_languages.update(episode.get("languages", []))

    anime_out = {
        "anime_id": anime_id,
        "title": name,
        "original_title": catalogue["alternative_names"][0] if catalogue["alternative_names"] else None,
        "alternative_titles": catalogue["alternative_names"],
        "description": synopsis,
        "image": image_url,
        "image_url": image_url,
        "year": catalogue.get("year"),
        "status": None,
        "rating": 0,
        "featured": 0,
        "has_episodes": 1 if seasons_out else 0,
        "seasons_fetched": 1,
        "genres": catalogue["genres"],
        "languages": sorted(list(real_languages)) if real_languages else sorted(list(catalogue["languages"])),
        "seasons": seasons_out,
    }
    return anime_out


# ==================================================================
# State — utilisé uniquement pour le cache (last_scraped), plus pour les IDs
# ==================================================================
def load_state(state_path: str) -> dict:
    if os.path.exists(state_path):
        try:
            with open(state_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "last_full_scrape": 0,
        "last_incremental_scrape": 0,
        # V2.15 : on garde le cache URL → {anime_id, name, last_scraped, data}
        # mais anime_id est désormais calculé via stable_anime_id(url), pas via
        # un compteur. Le cache sert uniquement à skip les animes déjà scrapés
        # récemment (si on veut faire du增量).
        "animes_scraped": {},
        "catalogue_seen_urls": [],
    }


def save_state(state_path: str, state: dict):
    state["last_incremental_scrape"] = int(time.time())
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def write_db(db_path: str, animes: list[dict]):
    db_exists = os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    if not db_exists:
        log.info("Création d'une nouvelle DB %s ...", db_path)
        conn.executescript(SCHEMA)
    else:
        log.info("Mise à jour incrémentale de la DB %s ...", db_path)
        # Migration : ajouter la colonne alternative_titles si elle n'existe pas
        try:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(anime)").fetchall()]
            if "alternative_titles" not in cols:
                conn.execute("ALTER TABLE anime ADD COLUMN alternative_titles TEXT")
                log.info("  → Colonne 'alternative_titles' ajoutée à la table anime")
        except Exception as e:
            log.warning("Migration alternative_titles: %s", e)
    c = conn.cursor()
    # V2.15 : on vide toutes les tables avant de re-remplir pour éviter les
    # restes d'anciens IDs. Comme les IDs sont désormais stables, le INSERT OR
    # REPLACE aurait suffit, mais un TRUNCATE est plus propre.
    for table in ["episode_url", "episode", "season", "anime_genre", "anime", "genre", "discover"]:
        c.execute(f"DELETE FROM {table}")
    conn.commit()

    for anime in animes:
        _upsert_anime(c, anime)
    conn.commit()
    conn.close()


def _upsert_anime(c, anime: dict):
    anime_id = int(anime.get("anime_id") or anime.get("id", 0))
    if not anime_id:
        return
    if "image" in anime and anime["image"]:
        anime["image"] = fix_image_url(anime["image"])
    if "image_url" in anime and anime["image_url"]:
        anime["image_url"] = fix_image_url(anime["image_url"])

    c.execute("""
        INSERT OR REPLACE INTO anime
        (anime_id, title, title_normalized, original_title, alternative_titles,
         description, image, image_url, year, status, rating, featured,
         has_episodes, seasons_fetched, languages, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        anime_id, anime.get("title", ""), normalize(anime.get("title", "")),
        anime.get("original_title"),
        json.dumps(anime.get("alternative_titles", []), ensure_ascii=False),
        anime.get("description"),
        anime.get("image"), anime.get("image_url"), anime.get("year"),
        anime.get("status"), float(anime.get("rating") or 0),
        1 if anime.get("featured") else 0,
        1 if anime.get("has_episodes") or (anime.get("seasons") and len(anime["seasons"]) > 0) else 0,
        1 if anime.get("seasons_fetched") else 0,
        json.dumps(anime.get("languages", []), ensure_ascii=False),
        json.dumps(anime, ensure_ascii=False),
    ))

    for genre_name in anime.get("genres", []) or []:
        if not genre_name:
            continue
        genre_norm = normalize(genre_name)
        c.execute("INSERT OR IGNORE INTO genre (name, name_normalized) VALUES (?, ?)",
                  (genre_name, genre_norm))
        genre_id = c.execute("SELECT id FROM genre WHERE name_normalized = ?", (genre_norm,)).fetchone()[0]
        c.execute("INSERT OR IGNORE INTO anime_genre (anime_id, genre_id) VALUES (?, ?)", (int(anime_id), genre_id))

    # V2.15 : on DELETE par anime_id (stable désormais, pas de risque de résidu)
    season_ids = [row[0] for row in c.execute("SELECT id FROM season WHERE anime_id = ?", (int(anime_id),))]
    if season_ids:
        placeholders = ",".join("?" * len(season_ids))
        episode_ids = [row[0] for row in c.execute(f"SELECT id FROM episode WHERE season_id IN ({placeholders})", season_ids)]
        if episode_ids:
            ep_placeholders = ",".join("?" * len(episode_ids))
            c.execute(f"DELETE FROM episode_url WHERE episode_id IN ({ep_placeholders})", episode_ids)
        c.execute(f"DELETE FROM episode WHERE season_id IN ({placeholders})", season_ids)
    c.execute("DELETE FROM season WHERE anime_id = ?", (int(anime_id),))

    for season in anime.get("seasons", []) or []:
        season_number = season.get("season_number", 0)
        season_name = season.get("name", "")
        c.execute("INSERT OR IGNORE INTO season (anime_id, season_number, name) VALUES (?, ?, ?)",
                  (int(anime_id), int(season_number), season_name))
        season_id = c.execute(
            "SELECT id FROM season WHERE anime_id = ? AND season_number = ? AND name = ?",
            (int(anime_id), int(season_number), season_name)
        ).fetchone()[0]

        for episode in season.get("episodes", []) or []:
            ep_num = episode.get("episode_number", 0)
            c.execute("""
                INSERT OR IGNORE INTO episode
                (season_id, episode_number, title, description, duration, languages)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                season_id, int(ep_num), episode.get("title", ""),
                episode.get("description", ""), episode.get("duration", ""),
                json.dumps(episode.get("languages", []), ensure_ascii=False),
            ))
            episode_id = c.execute(
                "SELECT id FROM episode WHERE season_id = ? AND episode_number = ?",
                (season_id, int(ep_num))
            ).fetchone()[0]

            for lang, urls in (episode.get("urls") or {}).items():
                for pos, item in enumerate(urls):
                    u = item["url"] if isinstance(item, dict) else item
                    h = item["host"] if isinstance(item, dict) else extract_host(u)
                    if not u:
                        continue
                    c.execute("""
                        INSERT INTO episode_url (episode_id, language, url, url_position, host)
                        VALUES (?, ?, ?, ?, ?)
                    """, (episode_id, lang, u, pos, h))


async def run_scraper(args):
    state_path = args.state
    db_path = args.db
    json_path = args.json
    max_animes = args.max_animes

    state = load_state(state_path)
    log.info("State chargé : %d animes déjà connus, dernier scrape il y a %d min",
             len(state["animes_scraped"]),
             (int(time.time()) - state["last_incremental_scrape"]) // 60)

    if args.no_scrap:
        log.info("Mode --no-scrap : conversion du state en DB uniquement")
        animes_out = [info["data"] for info in state["animes_scraped"].values() if "data" in info]
        write_db(db_path, animes_out)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"anime": animes_out}, f, ensure_ascii=False, indent=2)
        return

    client = ScraperClient()

    try:
        site_url = await find_site_url(client)
        if not site_url:
            log.error("Abandon : domaine introuvable")
            return

        all_catalogues = await fetch_all_catalogues(client, site_url, max_animes)
        if not all_catalogues:
            log.error("Catalogue vide — abandon")
            return

        # V2.15 : tous les animes du catalogue sont scrapés (le state ne sert
        # plus qu'au cache). On peut ajouter une option --incremental plus tard
        # pour skip les animes scrapés il y a moins de X heures.
        catalogues_to_scrape = all_catalogues
        if max_animes:
            catalogues_to_scrape = catalogues_to_scrape[:max_animes]

        animes_out: list[dict] = []
        new_count = 0
        updated_count = 0
        unchanged_count = 0
        start_time = int(time.time())

        pbar = tqdm(
            catalogues_to_scrape,
            desc="Scraping",
            unit="anime",
            dynamic_ncols=True,
            bar_format="{desc} {percentage:3.0f}% |{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] ETA {remaining} | {postfix}",
        )
        for idx, cat in enumerate(pbar, start=1):
            url = cat["url"]
            # V2.15 : anime_id STABLE via hash MD5 de l'URL
            anime_id = stable_anime_id(url)
            state["animes_scraped"][url] = {
                "anime_id": anime_id,
                "name": cat["name"],
                "last_scraped": int(time.time()),
            }

            short_name = cat["name"][:40] + ("..." if len(cat["name"]) > 40 else "")
            pbar.set_postfix_str(f"{short_name} [id={anime_id}]")

            try:
                anime_data = await scrape_anime(client, cat, anime_id)
                old_data = state["animes_scraped"][url].get("data")
                if old_data:
                    changes = []
                    if old_data.get("title") != anime_data.get("title"):
                        changes.append(f"titre changé")
                    if old_data.get("description") != anime_data.get("description"):
                        changes.append("description changée")
                    if old_data.get("image") != anime_data.get("image"):
                        changes.append("image changée")
                    if set(old_data.get("genres", [])) != set(anime_data.get("genres", [])):
                        changes.append("genres changés")
                    old_eps = sum(len(s.get("episodes", [])) for s in old_data.get("seasons", []))
                    new_eps = sum(len(s.get("episodes", [])) for s in anime_data.get("seasons", []))
                    if new_eps > old_eps:
                        changes.append(f"+{new_eps - old_eps} épisode(s)")
                    old_seasons = len(old_data.get("seasons", []))
                    new_seasons = len(anime_data.get("seasons", []))
                    if new_seasons > old_seasons:
                        changes.append(f"+{new_seasons - old_seasons} saison(s)")
                    if changes:
                        for change in changes:
                            log.info("  → %s", change)
                        updated_count += 1
                    else:
                        unchanged_count += 1
                else:
                    new_count += 1
                state["animes_scraped"][url]["data"] = anime_data
                animes_out.append(anime_data)
                if idx % 10 == 0:
                    save_state(state_path, state)
            except Exception as e:
                log.error("  ✗ Erreur scrape %s : %s", url, e)
        pbar.close()

        state["catalogue_seen_urls"] = [c["url"] for c in all_catalogues]
        if not state["last_full_scrape"]:
            state["last_full_scrape"] = int(time.time())
        save_state(state_path, state)

        log.info("Écriture de la DB %s ...", db_path)
        write_db(db_path, animes_out)
        log.info("Écriture du JSON %s ...", json_path)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"anime": animes_out}, f, ensure_ascii=False, indent=2)

        log.info(
            "\n✅ Scrap terminé\n"
            "   Requêtes HTTP : %d\n"
            "   Animes nouveaux : %d\n"
            "   Animes mis à jour : %d\n"
            "   Animes inchangés : %d\n"
            "   Total DB : %d animes\n"
            "   Durée : %d s",
            client.req_count, new_count, updated_count, unchanged_count,
            len(animes_out), int(time.time()) - start_time,
        )
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Scraper Anime-Sama → animezone.db + animezone.json")
    parser.add_argument("--db", default="animezone.db", help="Chemin DB SQLite de sortie")
    parser.add_argument("--json", default="animezone.json", help="Chemin JSON de sortie")
    parser.add_argument("--state", default="state.json", help="Chemin state.json persistant")
    parser.add_argument("--max-animes", type=int, default=None, help="Limite (pour test)")
    parser.add_argument("--no-scrap", action="store_true", help="Convertir state → DB sans scraper")

    # Options HuggingFace sync
    parser.add_argument("--hf", help="Token HuggingFace pour sync cloud")
    parser.add_argument("--repo", default="animezone-catalog", help="Nom du repo HF")
    parser.add_argument("--push", action="store_true", help="Push DB + state sur HF a la fin")
    parser.add_argument("--pull", action="store_true", help="Pull state depuis HF au debut")

    args = parser.parse_args()

    # Setup HF si token fourni
    hf_api = None
    hf_repo = ""
    if args.hf:
        hf_api = HfApi(token=args.hf)
        try:
            user_info = whoami(token=args.hf)
            username = user_info["name"]
            hf_repo = f"{username}/{args.repo}" if "/" not in args.repo else args.repo
            hf_api.create_repo(repo_id=hf_repo, repo_type="dataset", private=True, exist_ok=True)
            log.info("HF sync active sur %s", hf_repo)
        except Exception as e:
            log.error("Erreur init HF: %s", e)
            hf_api = None

    # Pull state depuis HF si demandé
    if args.pull and hf_api:
        try:
            log.info("Telechargement state.json depuis HF ...")
            path = hf_hub_download(repo_id=hf_repo, filename="state.json", repo_type="dataset", token=args.hf)
            import shutil
            shutil.copy(path, args.state)
            log.info("✓ state.json recupere")
        except Exception:
            log.info("Pas de state.json sur HF — demarrage from scratch")

    asyncio.run(run_scraper(args))

    # Push vers HF si demandé
    if args.push and hf_api:
        log.info("Push des resultats sur HF ...")
        if os.path.exists(args.db):
            hf_api.upload_file(path_or_fileobj=args.db, path_in_repo="animezone.db", repo_id=hf_repo, repo_type="dataset")
            log.info("✓ animezone.db pousse")
        if os.path.exists(args.state):
            hf_api.upload_file(path_or_fileobj=args.state, path_in_repo="state.json", repo_id=hf_repo, repo_type="dataset")
            log.info("✓ state.json pousse")
        # Generer et pousser le manifest
        import sqlite3, time
        if os.path.exists(args.db):
            conn = sqlite3.connect(args.db)
            c = conn.cursor()
            # V2.15 : calcul du SHA256 de la DB pour vérification d'intégrité côté app
            import hashlib as _hl
            sha256 = _hl.sha256()
            with open(args.db, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    sha256.update(chunk)
            manifest = {
                "db_version": int(time.time()),
                "last_update": int(time.time()),
                "total_animes": c.execute("SELECT COUNT(*) FROM anime").fetchone()[0],
                "total_episodes": c.execute("SELECT COUNT(*) FROM episode").fetchone()[0],
                "total_urls": c.execute("SELECT COUNT(*) FROM episode_url").fetchone()[0],
                "db_sha256": sha256.hexdigest(),
            }
            conn.close()
            with open("/tmp/manifest.json", "w") as f:
                json.dump(manifest, f, indent=2)
            hf_api.upload_file(path_or_fileobj="/tmp/manifest.json", path_in_repo="manifest.json", repo_id=hf_repo, repo_type="dataset")
            log.info("✓ manifest.json pousse: %d animes, %d eps, sha256=%s...",
                     manifest["total_animes"], manifest["total_episodes"], manifest["db_sha256"][:16])


if __name__ == "__main__":
    main()
