#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrap_experimental.py — Scraper Anime-Sama avec bypass Cloudflare en cascade.

OBJECTIF : éliminer les erreurs 403 sur les GitHub Runners (8/9 bloqués).

STRATÉGIE EN 4 NIVEAUX (escalade automatique) :
  1. DIRECT    : curl_cffi avec impersonation TLS d'un vrai navigateur
                 (Chrome 131). Passe sur les runners dont l'IP n'est pas
                 flagrée. Très rapide (~0.3 s/requête).
  2. COOKIES   : un vrai navigateur (FlareSolverr si dispo, sinon Playwright
                 Chromium intégré) résout le challenge Cloudflare UNE fois.
                 Les cookies cf_clearance obtenus sont réinjectés dans curl_cffi
                 pour TOUTES les requêtes suivantes → rapide à nouveau.
                 Re-solve automatique si la clearance expire.
  3. PROXYSTREAM : changement d'IP virtuel par proxys HTTP publics.
                 Modèle "validation par l'usage" (le pool pré-validé échoue :
                 un proxy gratuit meurt en 30-60 s après validation) :
                   · liste CHAUTE = proxys ayant réussi une VRAIE requête il y
                     a < 2,5 min → utilisés en priorité (1 requête à la fois
                     par proxy : ils suffoquent en concurrence)
                   · sinon candidat frais jamais testé, essayé directement sur
                     l'URL cible ; s'il réussit il rejoint la liste chaude
                   · tout échec → proxy marqué mort à vie, candidat suivant
                 Session curl_cffi FRAÎCHE par requête (réutiliser une session
                 créée sans proxy avec override `proxies` casse le keep-alive
                 curl → timeouts).
                 PROXY_URL (proxy résidentiel perso) est utilisé seul s'il
                 est fourni.
  4. RELAIS    : relais publics (allorigins → codetabs → r.jina.ai) en
                 dernier recours.

USAGE IDENTIQUE À scrap.py (mêmes arguments) :
  python scrap_experimental.py --state state.json --db animezone.db \
      --json animezone.json --max-animes 5          # test rapide
  python scrap_experimental.py --state ... --push --hf $HF_TOKEN   # production

VARIABLES D'ENV (optionnelles) :
  FLARESOLVERR_URL   ex: http://localhost:8191  (service FlareSolverr actif)
  PROXY_URL          ex: http://user:pass@host:port (proxy résidentiel perso,
                     remplace le ProxyStream au niveau 3)
  NO_BROWSER=1       désactive le solveur navigateur intégré
  NO_PROXYSTREAM=1   désactive les proxys publics (niveau 3)
  NO_RELAY=1         désactive les relais publics
  DEBUG_CF=1         logs verbeux

EXEMPLE GITHUB ACTIONS avec FlareSolverr :
  services:
    flaresolverr:
      image: ghcr.io/flaresolverr/flaresolverr:latest
      ports: ["8191:8191"]
  env:
    FLARESOLVERR_URL: http://localhost:8191

Le reste du pipeline (parse, DB, state, push HuggingFace, tri des lecteurs,
images HD) est RÉUTILISÉ depuis scrap.py — aucun duplicata.
"""
import asyncio
import concurrent.futures as cf
import json
import logging
import os
import random
import re
import sys
import threading
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrap  # noqa: E402  — réutilise parse/write_db/state/HF push & pull

log = logging.getLogger("scrap_exp")

try:
    from curl_cffi.requests import Session as CurlSession
except ImportError:
    log.critical("curl_cffi requis : pip install curl_cffi")
    sys.exit(1)

try:
    import requests as _requests
except ImportError:
    _requests = None

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "").rstrip("/")
PROXY_URL = os.environ.get("PROXY_URL", "")
NO_BROWSER = os.environ.get("NO_BROWSER", "") == "1"
NO_PROXYSTREAM = os.environ.get("NO_PROXYSTREAM", "") == "1"
NO_RELAY = os.environ.get("NO_RELAY", "") == "1"
DEBUG_CF = os.environ.get("DEBUG_CF", "") == "1"

# UA unifié : forcé partout (curl_cffi ET navigateur solveur) pour que la
# clearance cf_clearance soit liée au même UA que les requêtes rapides.
UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

IMPERSONATIONS = ["chrome131", "chrome124", "safari17_0", "firefox133"]

# Domaines anime-sama (testés en live : .fr/.eu n'existent pas, .to actif)
SAMA_DOMAINS = ["https://anime-sama.to/", "https://anime-sama.org/",
                "https://anime-sama.si/", "https://anime-sama.tv/"]

# Sources de listes de proxys HTTP publics (gratuites, sans clé)
PROXY_SOURCES = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http"
    "&timeout=5000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

RE_CHALLENGE = re.compile(
    r"just a moment|attention required|challenge-platform|cf-browser-verification"
    r"|enable javascript and cookies|cloudflare-nginx",
    re.IGNORECASE,
)

RELAY_DELAY = 0.8    # délai entre requêtes via relais (politesse)
PROXY_DELAY = 0.6    # délai entre requêtes via proxys
SOLVE_MAX = 3        # nombre max de solves par job
PROXY_ATTEMPTS = 10  # tentatives max par URL en mode proxys
# Timeouts LARGE côté requêtes : les pages anime ne sont pas dans le cache
# Cloudflare (contrairement à /catalogue/ qui valide les proxys) → réponse
# origine plus lente à travers des proxys publics lents. 8-14 s = timeouts
# à tort ; testé OK à 20-25 s.
PROXY_TIMEOUT = 25   # s — timeout requête via proxy (chaud ou PROXY_URL)


def classify_response(status: int, text: str) -> str:
    """ok | challenge | blocked | ratelimit | error"""
    snippet = text[:2000] if text else ""
    if status == 200:
        if len(text) < 50 and RE_CHALLENGE.search(snippet):
            return "challenge"
        return "ok"
    if status in (403, 503):
        if RE_CHALLENGE.search(snippet):
            return "challenge"
        return "blocked"
    if status == 429:
        return "ratelimit"
    return "error"


# ------------------------------------------------------------------
# ProxyStream (niveau 3) — changement d'IP virtuel, validateur continu
# ------------------------------------------------------------------
class ProxyStream:
    """Proxys HTTP publics avec validateur continu en tâche de fond.

    Constat mesuré (2026-09-26) : ~7 % des proxys des listes publiques
    passent Cloudflare à un instant T, et un proxy qui passe meurt en
    30-90 s. Un pool pré-validé une seule fois est donc toujours périmé.

    Architecture :
      · THREAD VALIDATEUR (fond) : sonde les candidats en parallèle
        (30 workers, requête /catalogue/ réelle 358 Ko → prouve réseau
        ET passage CF) et maintient une liste chaude ~10 proxys.
      · LISTE CHAUDE : proxys prouvés il y a < HOT_TTL. Chaque proxy ne
        porte qu'UNE requête à la fois (ils suffoquent en concurrence).
      · REQUÊTES : piochent dans la liste chaude (attente max ACQUIRE_WAIT
        si elle se vide) ; tout échec → blacklist à vie + éviction.
      · Session curl_cffi FRAÎCHE par requête (réutiliser une session
        créée sans proxy avec override `proxies` casse le keep-alive).
    """

    HOT_TARGET = 16       # liste chaude visée par le validateur (marge > 4 workers)
    HOT_TTL = 150         # s — un proxy chaud meurt sans succès depuis…
    ACQUIRE_WAIT = 90     # s — attente max d'un proxy chaud disponible
                          # (le préchauffage initial prend 30-60 s : listes
                          # ~20 s + 1er lot de sondes ~15 s)
    PROBE_WORKERS = 30
    # Sonde en 2 ÉTAGES :
    #   1. 404 légère (1,9 Ko) — filtre CF cheap : prouve proxy → Cloudflare
    #      → origine. Une IP flagrée CF reçoit 403/challenge, jamais ce 404.
    #   2. /catalogue/ (358 Ko, cachée CF) — filtre DÉBIT sur les survivants
    #      (~3 %) : un proxy qui sait passer CF mais pas transporter 100 Ko
    #      plante les vraies pages (constaté : pages saison en timeout alors
    #      que la sonde légère passait).
    # La bande passante consommée reste minime : ~30×1,9 Ko + ~1×358 Ko par
    # lot (crucial sur les environnements à faible débit — sandbox 0,45 Mo/s).
    PROBE_URL = "https://anime-sama.to/404-inexistant/"
    PROBE_NEEDLE = "Accès Introuvable"
    PROBE2_URL = "https://anime-sama.to/catalogue/"
    PROBE2_NEEDLE = "card-title"

    def __init__(self):
        self.hot: list[dict] = []          # {"proxy", "busy", "last_ok"}
        self.dead: set[str] = set()
        self._buffer: list[str] = []
        self._lock = threading.Lock()
        self._rr = 0
        self._thread: threading.Thread | None = None
        self.stats = {"probed": 0, "validated": 0, "served": 0, "died": 0,
                      "list_fetch": 0}

    # ---------- synchrone (exécuté dans le thread validateur) ----------
    @staticmethod
    def _fetch_candidates(limit: int = 1500) -> list[str]:
        found: set[str] = set()
        for src in PROXY_SOURCES:
            if len(found) >= limit:
                break
            try:
                req = urllib.request.Request(src, headers={"User-Agent": UA_CHROME})
                with urllib.request.urlopen(req, timeout=25) as r:
                    txt = r.read().decode("utf-8", errors="replace")
                for line in txt.splitlines():
                    line = line.strip()
                    if re.match(r"^\d+\.\d+\.\d+\.\d+:\d+$", line):
                        found.add(line)
            except Exception as e:
                log.info("Liste proxys KO (%s…) : %s", src.split("/")[2], str(e)[:60])
        return list(found)[:limit]

    def _probe(self, proxy: str) -> float | None:
        """Sonde 2 étages via le proxy → durée totale en s, ou None.
        Étage 1 : 404 légère (1,9 Ko) = passage Cloudflare.
        Étage 2 : /catalogue/ (358 Ko) = capacité de débit réelle.
        Seuls les proxys qui transportent une vraie page entrent en liste
        chaude — c'est ce qui garantit qu'ils servent les pages anime."""
        try:
            s = CurlSession(
                impersonate="chrome131",
                proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            )
            t0 = time.monotonic()
            r = s.get(self.PROBE_URL, timeout=12,
                      headers={"Accept-Language": "fr-FR,fr;q=0.9"})
            if not (r.status_code == 404 and self.PROBE_NEEDLE in r.text):
                s.close()
                return None
            r = s.get(self.PROBE2_URL, timeout=20,
                      headers={"Accept-Language": "fr-FR,fr;q=0.9"})
            dt = time.monotonic() - t0
            s.close()
            if r.status_code == 200 and self.PROBE2_NEEDLE in r.text:
                return dt
            return None
        except Exception:
            return None

    def _validator_loop(self):
        """Boucle de fond : maintient la liste chaude en sondant en continu."""
        while True:
            try:
                now = time.monotonic()
                with self._lock:
                    self.hot = [h for h in self.hot
                                if now - h["last_ok"] < self.HOT_TTL]
                    need = max(0, self.HOT_TARGET - len(self.hot))
                if need == 0:
                    time.sleep(2)
                    continue
                # compléter le buffer si bas
                with self._lock:
                    buf_len = len(self._buffer)
                if buf_len < self.PROBE_WORKERS * 2:
                    cands = self._fetch_candidates(1500)
                    with self._lock:
                        known = self.dead | set(self._buffer)
                        fresh = [c for c in cands if c not in known]
                        random.shuffle(fresh)
                        self._buffer.extend(fresh)
                        self.stats["list_fetch"] += 1
                    log.info("ProxyStream : buffer rechargé (+%d)", len(fresh))
                # prendre un lot de candidats jamais testés
                with self._lock:
                    batch: list[str] = []
                    while self._buffer and len(batch) < self.PROBE_WORKERS:
                        c = self._buffer.pop(0)
                        if c not in self.dead:
                            batch.append(c)
                if not batch:
                    time.sleep(3)
                    continue
                # sonder le lot en parallèle
                with cf.ThreadPoolExecutor(max_workers=self.PROBE_WORKERS) as ex:
                    results = list(ex.map(self._probe, batch))
                with self._lock:
                    for proxy, speed in zip(batch, results):
                        self.stats["probed"] += 1
                        if speed is not None:
                            self.stats["validated"] += 1
                            self.hot.append(
                                {"proxy": proxy, "busy": False,
                                 "last_ok": time.monotonic(), "speed": speed})
                            log.info("  ✓ proxy chaud : %s (%.1fs) [%d/%d]",
                                     proxy, speed, len(self.hot), self.HOT_TARGET)
                        else:
                            self.dead.add(proxy)
            except Exception as e:
                log.warning("Validateur proxys : %s", str(e)[:100])
                time.sleep(5)

    # ---------- asyncio (chemin des requêtes) ----------
    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._validator_loop, daemon=True, name="proxy-validator")
            self._thread.start()
            log.info("ProxyStream : validateur de fond démarré")

    async def acquire(self) -> dict | None:
        """Le proxy chaud libre LE PLUS RAPIDE (attente jusqu'à ACQUIRE_WAIT).
        Les proxys triés par vitesse de leur dernier succès : les rapides
        (<3 s) enchaînent les pages, les lents meurent entre deux."""
        self.start()
        t0 = time.monotonic()
        while time.monotonic() - t0 < self.ACQUIRE_WAIT:
            with self._lock:
                now = time.monotonic()
                self.hot = [h for h in self.hot
                            if now - h["last_ok"] < self.HOT_TTL]
                free = [h for h in self.hot if not h["busy"]]
                if free:
                    free.sort(key=lambda h: h.get("speed", 99))
                    h = free[0]
                    h["busy"] = True
                    self.stats["served"] += 1
                    return h
            await asyncio.sleep(0.4)
        return None

    async def report(self, entry: dict, ok: bool, elapsed: float | None = None):
        with self._lock:
            entry["busy"] = False
            if ok:
                entry["last_ok"] = time.monotonic()
                if elapsed is not None:
                    # moyenne glissante : la vitesse réelle affine le score
                    prev = entry.get("speed") or elapsed
                    entry["speed"] = prev * 0.5 + elapsed * 0.5
                return
            self.stats["died"] += 1
        self.dead.add(entry["proxy"])
        with self._lock:
            self.hot = [h for h in self.hot if h["proxy"] != entry["proxy"]]
        if DEBUG_CF:
            log.info("Proxy %s mort (blacklisté)", entry["proxy"])

    def summary(self) -> dict:
        now = time.monotonic()
        with self._lock:
            hot = sum(1 for h in self.hot if now - h["last_ok"] < self.HOT_TTL)
            return {**self.stats, "hot": hot, "dead": len(self.dead),
                    "buffer": len(self._buffer)}


# ------------------------------------------------------------------
# Solveur de challenge Cloudflare (niveau 2)
# ------------------------------------------------------------------
class ChallengeSolver:
    def __init__(self):
        self.solves = 0

    def solve(self, url: str):
        """Retourne (cookies: dict, user_agent: str) ou None."""
        if self.solves >= SOLVE_MAX:
            return None
        self.solves += 1
        result = None
        if FLARESOLVERR_URL:
            log.info("Solve challenge via FlareSolverr (%s)…", FLARESOLVERR_URL)
            result = self._solve_flaresolverr(url)
        if result is None and not NO_BROWSER:
            log.info("Solve challenge via navigateur intégré (Playwright)…")
            result = self._solve_playwright(url)
        if result:
            cookies, ua = result
            cf_names = [n for n in cookies if n.startswith("cf_") or n == "__cf_bm"]
            log.info("✓ Challenge résolu — cookies: %s", cf_names)
            return cookies, ua
        log.warning("✗ Solve impossible (FlareSolverr=%s, Playwright=%s)",
                    bool(FLARESOLVERR_URL), not NO_BROWSER)
        return None

    def _solve_flaresolverr(self, url: str):
        if _requests is None:
            return None
        try:
            r = _requests.post(
                f"{FLARESOLVERR_URL}/v1",
                json={"cmd": "request.get", "url": url, "maxTimeout": 60000},
                timeout=75,
            )
            d = r.json()
            if d.get("status") == "ok":
                sol = d["solution"]
                cookies = {c["name"]: c["value"] for c in sol.get("cookies", [])}
                return cookies, sol.get("userAgent") or UA_CHROME
        except Exception as e:
            log.warning("FlareSolverr erreur: %s", str(e)[:100])
        return None

    def _solve_playwright(self, url: str):
        try:
            return self._solve_playwright_sync(url)
        except Exception as e:
            log.warning("Playwright erreur: %s", str(e)[:120])
            return None

    def _solve_playwright_sync(self, url: str):
        from playwright.sync_api import sync_playwright

        launch_kw = {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        }
        if PROXY_URL:
            launch_kw["proxy"] = {"server": PROXY_URL}
        with sync_playwright() as p:
            browser = p.chromium.launch(**launch_kw)
            ctx = browser.new_context(
                user_agent=UA_CHROME, locale="fr-FR",
                viewport={"width": 1366, "height": 900},
            )
            page = ctx.new_page()
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
            except Exception:
                pass
            ok = False
            for _ in range(14):  # jusqu'à ~42 s de challenge
                title = (page.title() or "").lower()
                if "moment" not in title and "attention" not in title \
                        and len(page.content()) > 5000:
                    ok = True
                    break
                page.wait_for_timeout(3000)
            cookies = {c["name"]: c["value"] for c in ctx.cookies()} if ok else {}
            browser.close()
            return (cookies, UA_CHROME) if ok and "cf_clearance" in cookies else None


# ------------------------------------------------------------------
# Client hybride (interface identique à scrap.ScraperClient)
# ------------------------------------------------------------------
class HybridClient:
    POOL_SIZE = 4

    def __init__(self):
        proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        self._sessions = [
            CurlSession(impersonate=imp, proxies=proxies)
            for imp in IMPERSONATIONS
        ]
        self._session_idx = 0
        self._session_lock = asyncio.Lock()
        self._concurrency = asyncio.Semaphore(self.POOL_SIZE)

        self._cookies = {}        # cf_clearance & co (niveau 2)
        self._ua = None           # UA lié à la clearance
        self._mode = "direct"     # direct | cookies | proxy | relay
        self._mode_lock = asyncio.Lock()
        self._relay_order = ["allorigins", "codetabs", "jina"]
        self._relay_fail = {r: 0 for r in self._relay_order}
        self._solver = ChallengeSolver()
        # niveau 3 : ProxyStream (ou PROXY_URL fixe s'il est fourni)
        self._stream = None if (NO_PROXYSTREAM or PROXY_URL) else ProxyStream()
        self._empty_streak = 0

        self.req_count = 0
        self.stats = {"direct": 0, "cookies": 0, "proxy": 0, "relay": 0,
                      "solve_fail": 0}
        # compat find_site_url original : self._sessions[0].get(url).url
        self._compat_session = _CompatSession(self)

    async def _get_session(self):
        async with self._session_lock:
            s = self._sessions[self._session_idx]
            self._session_idx = (self._session_idx + 1) % len(self._sessions)
            return s

    # ---------------- niveaux 1/2 : curl_cffi (direct ou cookies) --------
    async def _curl_get(self, url: str, timeout: int = 30):
        session = await self._get_session()
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        }
        if self._mode == "cookies" and self._ua:
            headers["User-Agent"] = self._ua
        resp = await asyncio.to_thread(
            session.get, url,
            headers=headers,
            cookies=self._cookies if self._mode == "cookies" else None,
            timeout=timeout,
            allow_redirects=True,
        )
        return resp.status_code, resp.text

    # ---------------- niveau 3 : requête via un proxy --------------------
    async def _proxy_get(self, url: str, entry: dict | None):
        """entry=None → PROXY_URL fixe ; sinon proxy du ProxyStream.
        Session curl_cffi FRAÎCHE par requête (réutiliser une session créée
        sans proxy avec override `proxies` casse le keep-alive curl)."""
        if entry is None:
            proxy = PROXY_URL
        else:
            proxy = entry["proxy"]
        kw = {"impersonate": "chrome131"}
        if proxy:
            kw["proxies"] = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        headers = {
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        }

        def _do():
            s = CurlSession(**kw)
            try:
                return s.get(url, headers=headers, timeout=PROXY_TIMEOUT,
                             allow_redirects=True)
            finally:
                s.close()

        try:
            resp = await asyncio.to_thread(_do)
            if DEBUG_CF:
                log.info("GET via %s → HTTP %d %s", proxy, resp.status_code, url[:55])
            return resp.status_code, resp.text
        except Exception as e:
            if DEBUG_CF:
                log.info("GET via %s → ÉCHEC %s", proxy, str(e)[:45])
            if entry is not None:
                await self._stream.report(entry, False)
            raise

    # ---------------- niveau 4 : relais publics ----------------
    async def _relay_get(self, url: str, timeout: int = 50):
        order = sorted(self._relay_order, key=lambda r: self._relay_fail[r])
        for relay in order:
            # r.jina.ai refuse application/javascript → HTML uniquement
            if relay == "jina" and ".js" in url:
                continue
            try:
                if relay == "allorigins":
                    wrapped = ("https://api.allorigins.win/get?url="
                               + urllib.parse.quote(url, safe=""))
                    status, body = await self._urllib_get(wrapped, timeout)
                    if status != 200:
                        raise RuntimeError(f"HTTP {status}")
                    d = json.loads(body)
                    inner = d.get("status", {}).get("http_code", 0)
                    text = d.get("contents", "")
                    # 404 = page absente légitime → renvoyer le corps
                    if inner not in (0, 200, 301, 302, 404):
                        raise RuntimeError(f"inner HTTP {inner}")
                    if inner == 404:
                        return "ok", text
                elif relay == "codetabs":
                    wrapped = ("https://api.codetabs.com/v1/proxy?quest="
                               + urllib.parse.quote(url, safe=""))
                    status, body = await self._urllib_get(wrapped, timeout)
                    if status != 200:
                        raise RuntimeError(f"HTTP {status}")
                    text = body
                else:  # jina — retourne le HTML si X-Return-Format: html
                    wrapped = "https://r.jina.ai/" + url
                    status, body = await self._urllib_get(
                        wrapped, timeout,
                        extra_headers={"X-Return-Format": "html"})
                    if status != 200:
                        raise RuntimeError(f"HTTP {status}")
                    text = body
                self._relay_fail[relay] = 0
                if classify_response(200, text) != "ok":
                    continue
                return "ok", text
            except Exception as e:
                if DEBUG_CF:
                    log.info("relais %s KO: %s", relay, str(e)[:80])
                self._relay_fail[relay] += 1
        return "blocked", ""

    @staticmethod
    async def _urllib_get(url: str, timeout: int, extra_headers: dict | None = None):
        def _do():
            headers = {"User-Agent": UA_CHROME}
            if extra_headers:
                headers.update(extra_headers)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read().decode("utf-8", errors="replace")
        return await asyncio.to_thread(_do)

    # ---------------- escalade ----------------
    async def _escalate(self, url: str):
        can_proxy = (self._stream is not None) or bool(PROXY_URL)
        async with self._mode_lock:
            if self._mode == "direct":
                self._mode = "cookies"
                log.info("Escalade niveau 2 : cookies navigateur")
                solved = await asyncio.to_thread(self._solver.solve, url)
                if solved:
                    self._cookies, self._ua = solved
                    return
                self.stats["solve_fail"] += 1
                if can_proxy:
                    log.info("Solve échoué → escalade niveau 3 : proxys")
                    self._mode = "proxy"
                elif not NO_RELAY:
                    log.info("Solve échoué → escalade niveau 4 : relais publics")
                    self._mode = "relay"
            elif self._mode == "cookies":
                solved = await asyncio.to_thread(self._solver.solve, url)
                if solved:
                    self._cookies, self._ua = solved
                    return
                self.stats["solve_fail"] += 1
                if can_proxy:
                    log.info("Clearance inefficace → escalade niveau 3 : proxys")
                    self._mode = "proxy"
                elif not NO_RELAY:
                    log.info("Clearance inefficace → escalade niveau 4 : relais")
                    self._mode = "relay"
            elif self._mode == "proxy":
                if NO_RELAY:
                    return
                log.info("Proxys indisponibles → escalade niveau 4 : relais")
                self._mode = "relay"
            # mode relay : rien à faire, rotation gérée par _relay_get

    # ---------------- interface publique ----------------
    async def get(self, url: str, *, retry: int = 6) -> str:
        async with self._concurrency:
            if self._mode == "proxy":
                retry = max(retry, PROXY_ATTEMPTS)
            for attempt in range(retry):
                delay = {"direct": scrap.REQUEST_DELAY, "cookies": scrap.REQUEST_DELAY,
                         "proxy": PROXY_DELAY, "relay": RELAY_DELAY}[self._mode]
                await asyncio.sleep(delay * (1 + attempt * 0.3))
                entry = None
                elapsed = None
                try:
                    if self._mode in ("direct", "cookies"):
                        status, text = await self._curl_get(url)
                        self.req_count += 1
                        self.stats["direct" if self._mode == "direct" else "cookies"] += 1
                    elif self._mode == "proxy":
                        entry = await self._stream.acquire()
                        if entry is None:
                            # Escalade vers relais SEULEMENT si le validateur
                            # est mature (a déjà sondé beaucoup) ET vide 3 fois
                            # de suite : au démarrage la liste chaude met
                            # 30-60 s à préchauffer, ce n'est PAS une panne.
                            self._empty_streak += 1
                            mature = self._stream.stats["probed"] > 500
                            if mature and self._empty_streak >= 3:
                                self._empty_streak = 0
                                log.warning("ProxyStream vide malgré %d sondes "
                                            "→ escalade relais", self._stream.stats["probed"])
                                await self._escalate(url)
                                continue
                            log.warning("ProxyStream vide (essai %d, "
                                        "probed=%d) — on attend le validateur",
                                        self._empty_streak,
                                        self._stream.stats["probed"])
                            continue
                        self._empty_streak = 0
                        t0 = time.monotonic()
                        status, text = await self._proxy_get(url, entry)
                        elapsed = time.monotonic() - t0
                        self.req_count += 1
                        self.stats["proxy"] += 1
                    else:
                        verdict, text = await self._relay_get(url)
                        self.req_count += 1
                        self.stats["relay"] += 1
                        if verdict == "ok":
                            return text
                        # Les relais publics sont instables : si le pool de
                        # proxys a des chauds disponibles, on redescend.
                        if self._stream is not None:
                            with self._stream._lock:
                                n_hot = len(self._stream.hot)
                            if n_hot > 0:
                                log.info("Relais KO mais %d proxys chauds "
                                         "→ retour niveau 3", n_hot)
                                async with self._mode_lock:
                                    self._mode = "proxy"
                        continue
                    verdict = classify_response(status, text)
                    if status == 404:
                        # Page légitimement absente (saison/lang/vidéo non
                        # dispo sur le site) — comportement du client original :
                        # renvoyer le corps, le parseur filtre via
                        # "Page introuvable"/"Accès Introuvable". Retenter
                        # serait infini et brûlerait le pool de proxys.
                        if entry is not None:
                            await self._stream.report(entry, True, elapsed)
                        return text
                    if verdict == "ok":
                        validated = self._validate(url, text)
                        if validated is not None:
                            if entry is not None:
                                await self._stream.report(entry, True, elapsed)
                            return validated
                        if entry is not None:
                            await self._stream.report(entry, False)
                        continue  # contenu suspect (page quasi vide) → retry
                    if verdict in ("challenge", "blocked"):
                        if DEBUG_CF:
                            log.info("HTTP %d (%s) sur %s", status, verdict, url[:60])
                        if entry is not None:
                            # cette IP est flagrée CF → morte comme les autres
                            await self._stream.report(entry, False)
                        elif self._mode == "proxy":
                            await self._escalate(url)   # PROXY_URL fixe bloqué
                        else:
                            await self._escalate(url)   # direct/cookies bloqué
                        continue
                    # ratelimit / error → backoff
                    await asyncio.sleep(2 ** attempt + random.uniform(0, 1))
                except Exception as e:
                    log.warning("Erreur réseau %s: %s", url[:60], str(e)[:70])
                    await asyncio.sleep(1 + random.uniform(0, 1))
            log.error("Échec définitif après %d retries : %s", retry, url[:70])
            return ""

    @staticmethod
    def _validate(url: str, text: str) -> str | None:
        """Mêmes validations de contenu que le client original → str ou None."""
        if ".js" in url:
            if len(text) < 30 and "eps" not in text:
                return None
        else:
            if len(text) < 100 and "Page introuvable" not in text:
                return None
        return text

    def close(self):
        for s in self._sessions:
            try:
                s.close()
            except Exception:
                pass

    # compat : client._sessions[0].get(url, allow_redirects=True).url
    @property
    def compat(self):
        return self._compat_session


class _CompatSession:
    """Émule l'accès direct aux sessions pour find_site_url (original)."""

    def __init__(self, client: HybridClient):
        self._c = client

    def get(self, url: str, allow_redirects: bool = True, **kw):
        session = self._c._sessions[0]
        headers = {"User-Agent": UA_CHROME} if self._c._ua else None
        cookies = self._c._cookies if self._c._mode == "cookies" else None
        return session.get(url, headers=headers, cookies=cookies,
                           timeout=30, allow_redirects=allow_redirects)


# ------------------------------------------------------------------
# find_site_url hybride (sans accès _sessions direct)
# ------------------------------------------------------------------
async def find_site_url_hybrid(client) -> str | None:
    log.info("Recherche du domaine actif anime-sama (cascade anti-CF)…")
    for base in SAMA_DOMAINS:
        html = await client.get(base + "catalogue/?search=")
        if html and 'class="card-title"' in html:
            log.info("  ✓ Domaine actif : %s", base)
            return base
        log.info("  ✗ %s inaccessible", base)
    log.error("Aucun domaine Anime-Sama accessible")
    return None


# ------------------------------------------------------------------
# Branchement : monkey-patch puis on délègue TOUT à scrap.main()
# ------------------------------------------------------------------
def main():
    scrap.ScraperClient = HybridClient          # run_scraper utilisera le nôtre
    scrap.find_site_url = find_site_url_hybrid
    scrap.REQUEST_DELAY = max(0.45, scrap.REQUEST_DELAY)  # plus prudent en datacenter
    log.info("═══ scrap_experimental : cascade direct→cookies→proxys→relais ═══")
    log.info("FlareSolverr: %s | Proxy: %s | ProxyStream: %s | Browser: %s | Relais: %s",
             FLARESOLVERR_URL or "off", PROXY_URL or "off",
             "off" if (NO_PROXYSTREAM or PROXY_URL) else "on",
             "off" if NO_BROWSER else "on", "off" if NO_RELAY else "on")
    try:
        scrap.main()
    finally:
        stats = dict(HybridClient._last_stats or {})
        if HybridClient._last_client and HybridClient._last_client._stream:
            stats["proxystream"] = HybridClient._last_client._stream.summary()
        log.info("Stats requêtes : %s", json.dumps(stats, ensure_ascii=False))


# capture des stats du dernier client instancié (pour le résumé final)
_orig_init = HybridClient.__init__


def _patched_init(self):
    _orig_init(self)
    HybridClient._last_client = self


HybridClient.__init__ = _patched_init
HybridClient._last_stats = None
HybridClient._last_client = None


def _patched_close(self):
    try:
        st = self.stats
        log.info("Répartition requêtes : direct=%d cookies=%d proxys=%d relais=%d",
                 st["direct"], st["cookies"], st["proxy"], st["relay"])
        if self._stream:
            log.info("ProxyStream : %s", self._stream.summary())
        HybridClient._last_stats = st
    except Exception:
        pass
    _orig_close(self)


_orig_close = HybridClient.close
HybridClient.close = _patched_close

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG if DEBUG_CF else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    main()
