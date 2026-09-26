#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_scrape.py — Lanceur avec fallback anti-Cloudflare automatique.

POURQUOI CE WRAPPER ?
  scrap.py N'EXIT JAMAIS en erreur sur un 403 : il retente, puis logue
  « Échec définitif après N retries » et continue (exit 0). Un fallback
  bash naïf « scrap.py || scrap_experimental.py » ne se déclencherait
  donc JAMAIS. Ce wrapper streame la sortie de scrap.py en direct et
  détecte les signaux d'un blocage Cloudflare pour basculer au bon moment.

FLUX :
  Étape 1 — scrap.py (cloudscraper classique) avec tous les arguments
            reçus, SAUF --push (jamais pousser sur HuggingFace une DB
            potentiellement tronquée par des 403).
  Étape 2 — Analyse en direct des logs :
              · exit code != 0 (crash), OU
              · « Échec définitif après » (pages perdues après retries),
              · « Abandon : domaine introuvable » / « Catalogue vide »,
              · « Attention required » / « Just a moment » (pages CF),
              · DB générée avec 0 anime (catalogue entier bloqué)
            → si tout est propre → étape 3, sinon → étape 4.
  Étape 3 — Succès : régénère la DB depuis le state enrichi et pousse
            proprement : scrap.py --no-scrap <args --push>
            (zéro requête anime-sama → zéro risque de 403 au push).
  Étape 4 — Fallback : scrap_experimental.py (cascade curl_cffi →
            FlareSolverr/Playwright → proxys publics → relais) avec les
            mêmes arguments, --push inclus si demandé.
            Le state.json local enrichi par scrap.py est PRÉSERVÉ
            (--pull retiré si le state vient d'être modifié, sinon le
            pull HuggingFace écraserait le travail partiel).

Usage (mêmes arguments que scrap.py) :
  python run_scrape.py --push --pull --hf $HF_TOKEN
  python run_scrape.py --max-animes 3            # test rapide
"""
import argparse
import logging
import os
import sqlite3
import subprocess
import sys
import time

log = logging.getLogger("run_scrape")

# Signaux d'un blocage Cloudflare / pages définitivement perdues dans les
# logs de scrap.py. Un simple « Cloudflare block ... retry » isolé est
# ignoré (récupérable) ; seuls les échecs TERMINAUX déclenchent le fallback.
FAIL_PATTERNS = [
    "échec définitif après",          # page perdue après tous les retries
    "abandon : domaine introuvable",  # tous les domaines anime-sama bloqués
    "catalogue vide",                 # catalogue inaccessible
    "attention required",             # page challenge Cloudflare (titre)
    "just a moment",                  # challenge CF interstitiel (titre)
]


def run_step(cmd, scan_for=None):
    """Lance la commande, streame stdout+stderr en live (logs Actions),
    retourne (exit_code, lignes_suspectes)."""
    log.info("▶ Commande : %s", " ".join(cmd))
    t0 = time.time()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    hits = []
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        if scan_for:
            low = line.lower()
            for pat in scan_for:
                if pat in low:
                    hits.append(line.rstrip())
                    break
    code = proc.wait()
    log.info("■ Terminé en %d s — exit=%d, signaux CF=%d",
             int(time.time() - t0), code, len(hits))
    return code, hits


def build_args(args, unknown, push=False, pull=False, no_scrap=False):
    """Reconstruit la liste d'arguments pour les sous-scripts."""
    a = ["--db", args.db, "--json", args.json, "--state", args.state,
         "--repo", args.repo]
    if args.max_animes:
        a += ["--max-animes", str(args.max_animes)]
    if args.hf:
        a += ["--hf", args.hf]
    if no_scrap:
        a.append("--no-scrap")
    if pull:
        a.append("--pull")
    if push:
        a.append("--push")
    return a + unknown


def db_anime_count(db_path):
    """Nombre d'animes dans la DB (0 si vide, -1 si absente/illisible)."""
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        n = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return -1


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] run_scrape: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Lanceur scrap.py → fallback scrap_experimental.py")
    # mêmes options que scrap.py (transparence totale)
    parser.add_argument("--db", default="animezone.db")
    parser.add_argument("--json", default="animezone.json")
    parser.add_argument("--state", default="state.json")
    parser.add_argument("--max-animes", type=int, default=None)
    parser.add_argument("--no-scrap", action="store_true")
    parser.add_argument("--hf", default=None)
    parser.add_argument("--repo", default="animezone-catalog")
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--pull", action="store_true")
    args, unknown = parser.parse_known_args()
    if unknown:
        log.warning("Arguments non reconnus (forwardés tels quels) : %s", unknown)

    py = sys.executable or "python3"

    # ── Mode --no-scrap : aucune requête anime-sama → pas de fallback utile
    if args.no_scrap:
        code, _ = run_step([py, "scrap.py"] + build_args(
            args, unknown, push=args.push, pull=args.pull, no_scrap=True))
        sys.exit(code)

    state_mtime = os.path.getmtime(args.state) if os.path.exists(args.state) else 0

    # ── Étape 1 : scrap.py classique (sans --push)
    log.info("═══ Étape 1/2 : scrap.py (cloudscraper classique) ═══")
    code1, hits = run_step(
        [py, "scrap.py"] + build_args(args, unknown, pull=args.pull),
        scan_for=FAIL_PATTERNS,
    )
    state_frais = (os.path.exists(args.state)
                   and os.path.getmtime(args.state) > state_mtime)

    # ── Verdict
    animes = db_anime_count(args.db)
    problemes = []
    if code1 != 0:
        problemes.append(f"exit={code1}")
    if hits:
        problemes.append(f"{len(hits)} signal(aux) Cloudflare")
    if animes <= 0:
        problemes.append(f"DB {animes} anime(s) (catalogue bloqué ?)")
    for h in hits:
        log.warning("Signal CF : %s", h)

    if not problemes:
        # ── Étape 2 : succès propre → push sans re-scrap (zéro 403 possible)
        if args.push:
            log.info("═══ Étape 2/2 : scrap.py --no-scrap --push (push propre) ═══")
            code2, _ = run_step([py, "scrap.py"] + build_args(
                args, unknown, push=True, pull=False, no_scrap=True))
            sys.exit(code2)
        log.info("✅ Scrap propre, rien à faire de plus")
        sys.exit(0)

    # ── Fallback
    log.warning("⚠️ Problèmes détectés : %s → FALLBACK scrap_experimental.py",
                ", ".join(problemes))
    fb_args = build_args(args, unknown, push=args.push,
                         pull=args.pull and not state_frais)
    if not state_frais and args.pull:
        log.info("--pull conservé (state local intact)")
    elif state_frais:
        log.info("--pull retiré pour le fallback : le state.json local vient "
                 "d'être enrichi par scrap.py, un pull HF l'écraserait")

    log.info("═══ Fallback : scrap_experimental.py (cascade anti-Cloudflare) ═══")
    code_fb, _ = run_step([py, "scrap_experimental.py"] + fb_args)
    if code_fb == 0:
        log.info("✅ Fallback réussi — scrap anti-Cloudflare complet")
    else:
        log.error("❌ Fallback en échec aussi (exit=%d) — voir artifacts", code_fb)
    sys.exit(code_fb)


if __name__ == "__main__":
    main()
