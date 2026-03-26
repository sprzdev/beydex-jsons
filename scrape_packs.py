#!/usr/bin/env python3
"""
scrape_packs.py
Genera scraped_packs.json cruzando datos de la Wiki con los JSONs locales.

La Wiki bloquea requests directos (403). Solución: los datos ya están
hardcodeados aquí, extraídos manualmente desde el browser. Cuando salgan
packs nuevos, añádelos a TT_PACKS o HASBRO_PACKS y vuelve a ejecutar.

Uso:
  python3 scrape_packs.py
"""

import json
import re
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUTPUT   = Path(__file__).parent / "scraped_packs.json"

# ─────────────────────────────────────────────────────────────────────────────
# DATOS RAW — extraídos de la Wiki via browser
# Formato: (release_code, name, release_date, price_jpy, pack_type)
# pack_type: starter | booster | random_booster | deck_set | entry_set |
#            set | multipack | accessory | stadium | limited | prize
# ─────────────────────────────────────────────────────────────────────────────

TT_PACKS = [
    # ── BX Basic Line ────────────────────────────────────────────────────────
    ("BX-01",  "Starter DranSword 3-60F",              "2023-07-15", 1980,  "starter"),
    ("BX-02",  "Starter HellsScythe 4-60T",            "2023-07-15", 1980,  "starter"),
    ("BX-03",  "Starter WizardArrow 4-80B",             "2023-07-15", 1980,  "starter"),
    ("BX-04",  "Starter KnightShield 3-80N",            "2023-07-15", 1980,  "starter"),
    ("BX-05",  "Booster WizardArrow 4-80B",             "2023-07-15", 1400,  "booster"),
    ("BX-06",  "Booster KnightShield 3-80N",            "2023-07-15", 1400,  "booster"),
    ("BX-07",  "Start Dash Set",                        "2023-07-15", 5720,  "set"),
    ("BX-08",  "3on3 Deck Set",                         "2023-07-15", 3960,  "deck_set"),
    ("BX-09",  "Beybattle Pass",                        "2023-07-15", 3300,  "accessory"),
    ("BX-10",  "Xtreme Stadium",                        "2023-07-15", 2700,  "stadium"),
    ("BX-11",  "Launcher Grip",                         "2023-07-15", 700,   "accessory"),
    ("BX-12",  "3on3 Deck Case",                        "2023-07-15", 990,   "accessory"),
    ("BX-13",  "Booster KnightLance 4-80HN",            "2023-08-10", 1400,  "booster"),
    ("BX-14",  "Random Booster Vol. 1",                 "2023-09-09", 1400,  "random_booster"),
    ("BX-15",  "Starter LeonClaw 5-60P",                "2023-10-07", 1980,  "starter"),
    ("BX-16",  "Random Booster ViperTail Select",       "2023-10-07", 1400,  "random_booster"),
    ("BX-17",  "Battle Entry Set",                      "2023-10-07", 6050,  "entry_set"),
    ("BX-18",  "String Launcher",                       "2023-10-07", 990,   "accessory"),
    ("BX-19",  "Booster RhinoHorn 3-80S",               "2023-11-02", 1400,  "booster"),
    ("BX-20",  "DranDagger Deck Set",                   "2023-11-02", 3960,  "deck_set"),
    ("BX-21",  "HellsChain Deck Set",                   "2023-11-02", 3960,  "deck_set"),
    ("BX-22",  "DranSword 3-60F Entry Package",         "2023-12-02", 1400,  "booster"),
    ("BX-23",  "Starter PhoenixWing 9-60GF",            "2023-12-27", 2420,  "starter"),
    ("BX-24",  "Random Booster Vol. 2",                 "2023-12-27", 1400,  "random_booster"),
    ("BX-25",  "Gear Case",                             "2023-12-27", 4000,  "accessory"),
    ("BX-26",  "Booster UnicornSting 5-60GP",           "2024-01-27", 1400,  "booster"),
    ("BX-27",  "Random Booster SphinxCowl Select",      "2024-02-22", 1400,  "random_booster"),
    ("BX-28",  "String Launcher (White Ver.)",          "2024-03-30", 990,   "accessory"),
    ("BX-29",  "Custom Grip (White Ver.)",              "2024-03-30", 1100,  "accessory"),
    ("BX-30",  "Custom Grip (Red Ver.)",                "2024-03-30", 1100,  "accessory"),
    ("BX-31",  "Random Booster Vol. 3",                 "2024-04-27", 1400,  "random_booster"),
    ("BX-32",  "Wide Xtreme Stadium",                   "2024-07-13", 4400,  "stadium"),
    ("BX-33",  "Booster WeissTiger 3-60U",              "2024-06-15", 1400,  "booster"),
    ("BX-34",  "Starter CobaltDragoon 2-60C",           "2024-07-13", 2321,  "starter"),
    ("BX-35",  "Random Booster Vol. 4",                 "2024-07-13", 1400,  "random_booster"),
    ("BX-36",  "Random Booster WhaleWave Select",       "2024-09-14", 1400,  "random_booster"),
    ("BX-37",  "Double Xtreme Stadium Set",             "2024-10-12", 8200,  "stadium"),
    ("BX-38",  "Booster CrimsonGaruda 4-70TP",          "2024-11-02", 1400,  "booster"),
    ("BX-39",  "Random Booster ShelterDrake Select",    "2025-02-15", 1400,  "random_booster"),
    ("BX-40",  "Winder Launcher L",                     "2025-03-29", 880,   "accessory"),
    ("BX-41",  "Rubber Custom Grip (Gunmetal Ver.)",    "2025-03-29", 1320,  "accessory"),
    ("BX-42",  "Rubber Custom Grip (Blue Ver.)",        "2025-03-29", 1320,  "accessory"),
    ("BX-43",  "Gear Case (White Ver.)",                "2025-03-29", 4000,  "accessory"),
    ("BX-44",  "Booster TriceraPress M-85BS",           "2025-06-28", 1500,  "booster"),
    ("BX-45",  "Booster SamuraiCalibur 6-70M",         "2025-08-09", 1500,  "booster"),
    ("BX-46",  "Battle Entry Set Infinity",             "2025-10-11", 7500,  "entry_set"),
    ("BX-47",  "String Launcher L (Red Ver.)",          "2025-10-11", 990,   "accessory"),
    ("BX-48",  "Random Booster Vol. 9",                 "2026-02-14", 1600,  "random_booster"),
    ("BX-49",  "Starter DranStrike 4-50FF",             "2026-05-16", 2200,  "starter"),
    # ── UX Unique Line ────────────────────────────────────────────────────────
    ("UX-01",  "Starter DranBuster 1-60A",              "2024-03-30", 1980,  "starter"),
    ("UX-02",  "Starter HellsHammer 3-70H",             "2024-03-30", 1980,  "starter"),
    ("UX-03",  "Booster WizardRod 5-70DB",              "2024-03-30", 1400,  "booster"),
    ("UX-04",  "Battle Entry Set U",                    "2024-04-27", 6050,  "entry_set"),
    ("UX-05",  "Random Booster ShinobiShadow Select",   "2024-05-18", 1400,  "random_booster"),
    ("UX-06",  "Booster LeonCrest 7-60GN",              "2024-08-10", 1400,  "booster"),
    ("UX-07",  "PhoenixRudder Deck Set",                "2024-08-10", 4100,  "deck_set"),
    ("UX-08",  "Starter SilverWolf 3-80FB",             "2024-10-12", 2080,  "starter"),
    ("UX-09",  "Starter SamuraiSaber 2-70L",            "2024-11-02", 3300,  "starter"),
    ("UX-10",  "Customize Set U",                       "2024-11-02", 5700,  "set"),
    ("UX-11",  "Starter ImpactDrake 9-60LR",            "2024-12-28", 2420,  "starter"),
    ("UX-12",  "Random Booster Vol. 5",                 "2024-12-28", 1400,  "random_booster"),
    ("UX-13",  "Booster GolemRock 1-60UN",              "2025-01-25", 1400,  "booster"),
    ("UX-14",  "Starter ScorpioSpear 0-70Z",            "2025-04-26", 2200,  "starter"),
    ("UX-15",  "SharkScale Deck Set",                   "2025-08-09", 4200,  "deck_set"),
    ("UX-16",  "Random Booster ClockMirage Select",     "2025-10-11", 1400,  "random_booster"),
    ("UX-17",  "Starter MeteorDragoon 3-70J",           "2025-12-27", 2300,  "starter"),
    ("UX-18",  "Random Booster Vol. 8",                 "2025-12-27", 1600,  "random_booster"),
    ("UX-19",  "Starter BulletGriffon H",               "2026-04-25", 2600,  "starter"),
    # ── CX Custom Line ────────────────────────────────────────────────────────
    ("CX-01",  "Starter DranBrave S6-60V",              "2025-03-29", 2200,  "starter"),
    ("CX-02",  "Starter WizardArc R4-55LO",             "2025-03-29", 2200,  "starter"),
    ("CX-03",  "Booster PerseusDark B6-80W",            "2025-03-29", 1600,  "booster"),
    ("CX-04",  "Battle Entry Set C",                    "2025-03-29", 6501,  "entry_set"),
    ("CX-05",  "Random Booster Vol. 6",                 "2025-04-26", 1600,  "random_booster"),
    ("CX-06",  "Random Booster FoxBrush Select",        "2025-05-17", 1600,  "random_booster"),
    ("CX-07",  "Starter PegasusBlast ATr",              "2025-07-19", 2600,  "starter"),
    ("CX-08",  "Random Booster Vol. 7",                 "2025-07-19", 1600,  "random_booster"),
    ("CX-09",  "Starter SolEclipse D5-70TK",            "2025-09-27", 2500,  "starter"),
    ("CX-10",  "Booster WolfHunt F0-60DB",              "2025-11-01", 1700,  "booster"),
    ("CX-11",  "EmperorMight Deck Set",                 "2025-11-01", 5000,  "deck_set"),
    ("CX-12",  "Booster PhoenixFlare Z9-80WW",          "2026-01-24", 1700,  "booster"),
    ("CX-13",  "Starter BahamutBlitz BK1-50I",          "2026-03-28", 2200,  "starter"),
    ("CX-14",  "Starter KnightFortress GV8-70UN",       "2026-03-28", 2200,  "starter"),
    ("CX-15",  "Booster RagnaRage FE4-55Y",             "2026-03-28", 1600,  "booster"),
    ("CX-16",  "Start Dash Set C",                      "2026-03-28", 5650,  "set"),
]

HASBRO_PACKS = [
    # ── BX Starters ─────────────────────────────────────────────────────────
    ("F9580",  "Sword Dran 3-60F",             "2024-05-30", 16.99, "starter"),
    ("F9581",  "Helm Knight 3-80N",            "2024-05-30", 16.99, "starter"),
    ("F9582",  "Arrow Wizard 4-80B",           "2024-05-30", 16.99, "starter"),
    ("F9583",  "Scythe Incendio 4-60T",        "2024-05-30", 16.99, "starter"),
    ("G0184",  "Lance Knight 4-80HN",          "2024-07-01", 16.99, "starter"),
    ("G0193",  "Claw Leon 5-60P",              "2024-07-01", 16.99, "starter"),
    ("G1536",  "Buster Dran 1-60A",            "2024-12-01", 16.99, "starter"),
    ("G1537",  "Wand Wizard 5-70DB",           "2024-12-01", 16.99, "starter"),
    ("G1538",  "Wand Wizard 1-60R",            "2024-12-01", 16.99, "starter"),
    ("G1539",  "Shadow Shinobi 1-80MN",        "2024-12-01", 16.99, "starter"),
    ("G1673",  "Scarlet Garuda 4-70TP",        "2025-01-01", 16.99, "starter"),
    ("G1674",  "Sterling Wolf 3-80FB",         "2025-01-01", 16.99, "starter"),
    ("G1675",  "Shelter Drake 7-80GP",         "2025-01-01", 16.99, "starter"),
    ("G1676",  "Rock Golem 1-60UN",            "2025-01-01", 16.99, "starter"),
    ("G1751",  "Buster Dran 5-70DB",           "2025-01-01", 16.99, "starter"),
    ("G1752",  "Hammer Incendio 3-70H",        "2025-01-01", 16.99, "starter"),
    ("G2738",  "Stun Medusa 9-60GB",           "2026-01-30", 16.99, "starter"),
    ("G2739",  "Rudder Phoenix 4-70LF",        "2026-01-30", 16.99, "starter"),
    ("G2740",  "Feather Phoenix 2-60N",        "2026-01-30", 16.99, "starter"),
    # ── CX Starters ─────────────────────────────────────────────────────────
    ("G1677",  "Courage Dran S 6-60V",         "2025-01-01", 12.99, "starter"),
    ("G1678",  "Reaper Incendio T 4-70K",      "2025-01-01", 12.99, "starter"),
    ("G1679",  "Arc Wizard R 4-55LO",          "2025-01-01", 12.99, "starter"),
    ("G1680",  "Dark Perseus B 6-80W",         "2025-01-01", 12.99, "starter"),
    ("G1681",  "Brush Fox J 9-70GR",           "2025-01-01", 12.99, "starter"),
    ("G1682",  "Fort Hornet R 7-60T",          "2025-01-01", 12.99, "starter"),
    ("G1683",  "Wriggle Kraken S 3-85O",       "2025-01-01", 12.99, "starter"),
    ("G1684",  "Antler Stag B 2-60HN",         "2025-01-01", 12.99, "starter"),
    ("G2746",  "Reaper Rhino C 4-55D",         "2026-01-30", 14.99, "starter"),
    ("G2747",  "Flame Cerberus W 5-80WB",      "2026-01-30", 14.99, "starter"),
    ("G2748",  "Fang Leon T 4-60U",            "2026-01-30", 14.99, "starter"),
    # ── Infinity Starters ───────────────────────────────────────────────────
    ("G2742",  "Strike Dran 4-50FF",           "2026-07-15", 14.99, "starter"),
    ("G3497",  "Valor Bison FB",               "2026-07-15", 14.99, "starter"),
    ("G4561",  "Rocket Griffon H",             "2026-07-15", 14.99, "starter"),
    ("G4562",  "Rage Ragna FE 4-55Y",          "2026-07-15", 14.99, "starter"),
    ("G4563",  "Armor Knight GV 8-70UN",       "2026-07-15", 14.99, "starter"),
    ("G4571",  "Ring Aether 0-80DS",           "2026-07-15", 14.99, "starter"),
    ("G4572",  "Blitz Bahamut BK 1-50I",       "2026-07-15", 14.99, "starter"),
    # ── Boosters ────────────────────────────────────────────────────────────
    ("G0188",  "Steel Samurai 4-80T",          "2024-05-30", 11.99, "booster"),
    ("G0192",  "Horn Rhino 3-80S",             "2024-05-30", 11.99, "booster"),
    ("G0194",  "Keel Shark 3-60LF",            "2024-05-30", 11.99, "booster"),
    ("G0195",  "Talon Ptera 3-80B",            "2024-05-30", 11.99, "booster"),
    ("G0283",  "Sting Unicorn 5-60GP",         "2024-07-01", 11.99, "booster"),
    ("G0284",  "Roar Tyranno 9-60GF",          "2024-07-01", 11.99, "booster"),
    ("G0285",  "Scythe Incendio 3-80B",        "2024-07-01", 11.99, "booster"),
    ("G0286",  "Savage Bear 3-60S",            "2024-07-01", 11.99, "booster"),
    ("G1530",  "Cowl Sphinx 9-80GN",           "2024-12-01", 11.99, "booster"),
    ("G1531",  "Arrow Wizard 4-80GB",          "2024-12-01", 11.99, "booster"),
    ("G1533",  "Obsidian Shell 4-60D",         "2025-01-01", 11.99, "booster"),
    ("G1534",  "Keel Shark 1-60Q",             "2025-02-01", 11.99, "booster"),
    ("G1669",  "Tide Whale 5-80E",             "2025-01-01", 11.99, "booster"),
    ("G1670",  "Dagger Dran 4-70Q",            "2025-01-01", 11.99, "booster"),
    ("G1671",  "Lance Knight 3-60LF",          "2025-01-01", 11.99, "booster"),
    ("G1754",  "Yell Kong 3-60GB",             "2025-01-01", 11.99, "booster"),
    ("G1755",  "Arrow Wizard 4-80O",           "2025-01-01", 11.99, "booster"),
    ("G1756",  "Soar Phoenix 5-80H",           "2025-01-01", 11.99, "booster"),
    ("G2731",  "Scale Shark 4-50UF",           "2026-01-30",  9.99, "booster"),
    ("G2732",  "Shelter Drake 5-70O",          "2026-01-30",  9.99, "booster"),
    ("G2734",  "Curse Mummy 7-55W",            "2026-01-30",  9.99, "booster"),
    ("G3392",  "Ridge Triceratops 9-80GN",     "2026-01-30",  9.99, "booster"),
    # ── Dual Packs ──────────────────────────────────────────────────────────
    ("G0190",  "Knife Shinobi and Keel Shark Dual Pack",        "2024-05-30", 22.99, "multipack"),
    ("G0196",  "Chain Incendio and Arrow Wizard Dual Pack",     "2024-05-30", 22.99, "multipack"),
    ("G0197",  "Tail Viper and Sword Dran Dual Pack",           "2024-05-30", 22.99, "multipack"),
    ("G0198",  "Yell Kong and Helm Knight Dual Pack",           "2024-06-01", 22.99, "multipack"),
    ("G0199",  "Bite Croc and Sting Unicorn Dual Pack",         "2024-06-01", 22.99, "multipack"),
    ("G0282",  "Gale Wyvern and Tail Viper Dual Pack",          "2024-07-01", 22.99, "multipack"),
    ("G1542",  "Beat Tyranno and Knife Shinobi Dual Pack",      "2024-12-01", 22.99, "multipack"),
    ("G1543",  "Gale Wyvern and Sword Dran Dual Pack",          "2024-12-01", 22.99, "multipack"),
    ("G1685",  "Cowl Sphinx and Crest Leon Dual Pack",          "2025-01-01", 22.99, "multipack"),
    ("G1686",  "Pearl Tiger and Gill Shark Dual Pack",          "2025-01-01", 22.99, "multipack"),
    ("G1687",  "Circle Ghost and Chain Incendio Dual Pack",     "2025-01-01", 22.99, "multipack"),
    ("G1688",  "Tackle Goat and Sword Dran Dual Pack",          "2025-01-01", 22.99, "multipack"),
    ("G2754",  "Calibur Samurai and Obsidian Shell Dual Pack",  "2026-01-30", 17.99, "multipack"),
    ("G2755",  "Circle Ghost and Hack Viking Dual Pack",        "2026-01-30", 17.99, "multipack"),
    ("G2758",  "Spear Scorpio and Tail Viper Dual Pack",        "2026-01-30", 17.99, "multipack"),
    # ── Multipacks collab ───────────────────────────────────────────────────
    ("F9589",  "The Mandalorian and Moff Gideon Multipack Set",         "2024-06-01", 39.99, "multipack"),
    ("G0287",  "Iron Man and Thanos Multipack Set",                     "2024-06-01", 34.99, "multipack"),
    ("G0288",  "Spider-Man and Venom Multipack Set",                    "2024-06-01", 34.99, "multipack"),
    ("G0290",  "Luke Skywalker and Darth Vader Multipack Set",          "2024-06-01", 39.99, "multipack"),
    ("G0352",  "Optimus Primal and Starscream Multipack Set",           "2024-07-19", 34.99, "multipack"),
    ("G0353",  "Optimus Prime and Megatron Multipack Set",              "2024-07-19", 34.99, "multipack"),
    ("G1690",  "Captain America and Red Hulk Multipack Set",            "2025-06-01", 34.99, "multipack"),
    ("G1691",  "Miles Morales and Green Goblin Multipack Set",          "2025-06-01", 34.99, "multipack"),
    ("G1692",  "Bumblebee and Shockwave Multipack Set",                 "2025-06-01", 34.99, "multipack"),
    ("G1694",  "Obi-Wan Kenobi and General Grievous Multipack Set",     "2025-06-01", 39.99, "multipack"),
    ("G1695",  "Chewbacca and Stormtrooper Multipack Set",              "2025-09-01", 29.99, "multipack"),
    ("G1898",  "T. Rex and Mosasaurus Multipack Set",                   "2025-06-01", 34.99, "multipack"),
    ("G1899",  "Spinosaurus and Quetzalcoatlus Multipack Set",          "2025-06-01", 34.99, "multipack"),
    # ── Sets ────────────────────────────────────────────────────────────────
    ("G1844",  "Beyblade 25th Anniversary Set",    "2025-09-01", 109.99, "set"),
    ("G3195",  "X-treme Expansion Pack",           "2026-01-01",  29.99, "set"),
    ("G3393",  "Yggdrasil Team Pack",              "2026-01-01",  29.99, "set"),
    # ── Stadiums ────────────────────────────────────────────────────────────
    ("F9578",  "Beystadium",                       "2024-05-30",  16.99, "stadium"),
    ("G0318",  "Tournament Beystadium",            "2024-06-01",  69.99, "stadium"),
    ("G0841",  "Clash & Carry Beystadium",         "2025-01-20",  39.99, "stadium"),
    ("G1863",  "Wide Beystadium",                  "2025-10-01",  59.99, "stadium"),
    ("G1864",  "Double Xtreme Motorized Beystadium","2025-10-01", 89.99, "stadium"),
]

# ─────────────────────────────────────────────────────────────────────────────
# LOOKUPS de piezas (desde JSONs locales)
# ─────────────────────────────────────────────────────────────────────────────

BIT_ABBREVIATIONS = {
    "f": "Flat", "t": "Taper", "b": "Ball", "n": "Needle",
    "hn": "High Needle", "lf": "Low Flat", "p": "Point",
    "o": "Orb", "r": "Rush", "ht": "High Taper", "gf": "Gear Flat",
    "gb": "Gear Ball", "gn": "Gear Needle", "gp": "Gear Point",
    "gr": "Gear Rush", "tp": "Trans Point", "tk": "Trans Kick",
    "lr": "Low Rush", "q": "Quake", "s": "Spike", "a": "Accel",
    "d": "Dot", "db": "Disc Ball", "fb": "Free Ball",
    "uf": "Under Flat", "un": "Under Needle", "c": "Cyclone",
    "m": "Merge", "e": "Elevate", "v": "Vortex",
    "mn": "Metal Needle", "ff": "Free Flat", "fe": "Flow Edge",
    "ds": "Dual Spin", "gv": "Guard Vertical",
    "i": "Ignition", "j": "Jolt", "lo": "Low Orb",
    "bk": "Break Knuckle", "w": "Wedge", "wb": "Wall Ball",
    "ww": "Wall Wedge", "z": "Zap", "u": "Unite",
    "k": "Kick", "l": "Level", "bs": "Bound Spike",
    "y": "Yielding", "h": "Hexa",
}

ASSIST_LETTERS = {
    "s": "S (Slash)", "r": "R (Round)", "b": "B (Bumper)",
    "t": "T (Turn)", "c": "C (Charge)", "j": "J (Jaggy)",
    "a": "A (Assault)", "w": "W (Wheel)", "m": "M (Massive)",
    "d": "D (Dual)", "f": "F (Free)", "h": "H (Heavy)",
    "z": "Z (Zillion)", "k": "K (Knuckle)", "v": "V (Vertical)",
    "e": "E (Erase)",
}


def normalize(text: str) -> str:
    return re.sub(r"[\s\-_]", "", text.lower())


def load_local_data():
    data = {}
    for key, filename in [
        ("blades", "blades.json"), ("ratchets", "ratchets.json"),
        ("bits", "bits.json"), ("assistBlades", "assistBlades.json"),
        ("overBlades", "overBlades.json"),
    ]:
        path = DATA_DIR / filename
        with open(path, encoding="utf-8") as f:
            data[key] = json.load(f)
    return data


def build_lookup(local_data):
    lookup = {"blade": {}, "ratchet": {}, "bit": {}, "assist_blade": {}, "over_blade": {}}
    for b in local_data["blades"]:
        lookup["blade"][normalize(b["name"])] = b["id"]
        if b.get("hasbro_name"):
            lookup["blade"][normalize(b["hasbro_name"])] = b["id"]
    for r in local_data["ratchets"]:
        lookup["ratchet"][normalize(r["name"])] = r["id"]
    for b in local_data["bits"]:
        lookup["bit"][normalize(b["name"])] = b["id"]
        if b.get("full_name"):
            lookup["bit"][normalize(b["full_name"])] = b["id"]
        if b.get("abbreviation"):
            lookup["bit"][normalize(b["abbreviation"])] = b["id"]
    for ab in local_data["assistBlades"]:
        lookup["assist_blade"][normalize(ab["name"])] = ab["id"]
    for ob in local_data["overBlades"]:
        lookup["over_blade"][normalize(ob["name"])] = ob["id"]
    return lookup


def parse_combo(combo_text: str, lookup: dict) -> dict:
    """
    Parsea 'DranSword 3-60F' o 'DranBrave S6-60V' en IDs de piezas.
    """
    result = {k: None for k in ["blade_id", "ratchet_id", "bit_id", "assist_blade_id", "over_blade_id"]}
    if not combo_text:
        return result

    combo_text = combo_text.strip()
    ratchet_m = re.search(r'(\d+)-(\d+)', combo_text)

    if not ratchet_m:
        result["blade_id"] = lookup["blade"].get(normalize(combo_text))
        return result

    ratchet_str    = ratchet_m.group(0)
    before_ratchet = combo_text[:ratchet_m.start()].strip()
    after_ratchet  = combo_text[ratchet_m.end():].strip()

    # Detectar assist blade (letra(s) antes del número del ratchet)
    assist_m = re.search(r'\s+([A-Za-z]{1,3})$', before_ratchet)
    if assist_m:
        letter     = assist_m.group(1).lower()
        blade_name = before_ratchet[:assist_m.start()].strip()
        assist_name = ASSIST_LETTERS.get(letter)
        if assist_name:
            result["assist_blade_id"] = lookup["assist_blade"].get(normalize(assist_name))
    else:
        blade_name = before_ratchet

    result["blade_id"]   = lookup["blade"].get(normalize(blade_name))
    result["ratchet_id"] = lookup["ratchet"].get(normalize(ratchet_str))

    bit_abbr = after_ratchet.lower()
    bit_name = BIT_ABBREVIATIONS.get(bit_abbr)
    if bit_name:
        result["bit_id"] = lookup["bit"].get(normalize(bit_name))
    if not result["bit_id"] and bit_abbr:
        result["bit_id"] = lookup["bit"].get(normalize(bit_abbr))

    return result


NON_COMBO_KEYWORDS = re.compile(
    r'vol\.|set|stadium|launcher|grip|case|winder|pass|deck|entry|dash|'
    r'customize|multipack|dual pack|anniversary|expansion|team',
    re.IGNORECASE
)

PACK_PREFIXES = re.compile(
    r'^(Starter|Booster|Random Booster|Deck Set|Entry Set|Battle Entry Set|'
    r'Start Dash Set|Customize Set|PhoenixRudder|SharkScale|EmperorMight|'
    r'WolfHunt)\s+', re.IGNORECASE
)


def extract_combo(pack_name: str) -> str | None:
    if NON_COMBO_KEYWORDS.search(pack_name):
        return None
    combo = PACK_PREFIXES.sub("", pack_name).strip()
    # Si tras quitar el prefijo parece un combo (contiene N-NN), lo usamos
    if re.search(r'\d+-\d+', combo):
        return combo
    return None


def make_pack_id(release_code: str, name: str) -> str:
    code = re.sub(r'[^a-z0-9]', '', release_code.lower())
    # Packs BX-00 limitados: añadir slug del nombre
    if code in ("bx00", "ux00", "cx00"):
        slug = re.sub(r'[^a-z0-9]', '', name.lower())[:20]
        return f"pack-{code}-{slug}"
    return f"pack-{code}"


def detect_line(release_code: str) -> str:
    m = re.match(r'^([A-Z]+)', release_code)
    if m:
        pfx = m.group(1)
        if pfx in ("BX", "UX", "CX"):
            return pfx
    # Hasbro: inferir por rango de product code
    return "BX"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("🚀 BeyDeck Pack Builder")
    print("=" * 60)

    local_data = load_local_data()
    lookup = build_lookup(local_data)
    print(f"✅ Índice de piezas: {sum(len(v) for v in lookup.values())} entradas")

    packs      = []
    pack_parts = []
    stats      = {"matched": 0, "unmatched": 0}

    # Procesar TT
    for (code, name, date, price, ptype) in TT_PACKS:
        p = {
            "id": make_pack_id(code, name),
            "release_code": code,
            "name": name,
            "manufacturer": "takara_tomy",
            "type": ptype,
            "line": detect_line(code),
            "image": None,
            "price_jpy": price,
            "price_usd": None,
            "price_eur": None,
            "release_date": date,
            "release_notes": None,
            "is_available": True,
        }
        packs.append(p)
        combo = extract_combo(name)
        if combo:
            parts = parse_combo(combo, lookup)
            for pt, pid in [
                ("blade", parts["blade_id"]),
                ("ratchet", parts["ratchet_id"]),
                ("bit", parts["bit_id"]),
                ("assist_blade", parts["assist_blade_id"]),
                ("over_blade", parts["over_blade_id"]),
            ]:
                if pid:
                    pack_parts.append({
                        "pack_id": p["id"], "part_type": pt,
                        "part_id": pid, "is_guaranteed": True, "slot_group": None,
                    })
            stats["matched"] += 1
        else:
            stats["unmatched"] += 1

    # Procesar Hasbro
    for (code, name, date, price, ptype) in HASBRO_PACKS:
        p = {
            "id": make_pack_id(code, name),
            "release_code": code,
            "name": name,
            "manufacturer": "hasbro",
            "type": ptype,
            "line": detect_line(code),
            "image": None,
            "price_jpy": None,
            "price_usd": price,
            "price_eur": None,
            "release_date": date,
            "release_notes": None,
            "is_available": True,
        }
        packs.append(p)
        combo = extract_combo(name)
        if combo:
            parts = parse_combo(combo, lookup)
            for pt, pid in [
                ("blade", parts["blade_id"]),
                ("ratchet", parts["ratchet_id"]),
                ("bit", parts["bit_id"]),
                ("assist_blade", parts["assist_blade_id"]),
                ("over_blade", parts["over_blade_id"]),
            ]:
                if pid:
                    pack_parts.append({
                        "pack_id": p["id"], "part_type": pt,
                        "part_id": pid, "is_guaranteed": True, "slot_group": None,
                    })
            stats["matched"] += 1
        else:
            stats["unmatched"] += 1

    # Estadísticas de resolución de piezas
    resolved   = len([pp for pp in pack_parts if pp["part_id"]])
    unresolved = len([pp for pp in pack_parts if not pp["part_id"]])

    output = {
        "meta": {
            "total_packs":      len(packs),
            "takara_tomy":      len(TT_PACKS),
            "hasbro":           len(HASBRO_PACKS),
            "total_pack_parts": len(pack_parts),
            "combos_parsed":    stats["matched"],
            "no_combo":         stats["unmatched"],
            "parts_resolved":   resolved,
            "parts_unresolved": unresolved,
            "note": "Audita antes de ejecutar insert_packs.py. "
                    "Añade manualmente: Random Boosters en pack_parts, "
                    "ASINs en pack_affiliate_links."
        },
        "packs":               packs,
        "pack_parts":          pack_parts,
        "pack_beys":           [],
        "pack_affiliate_links": [],
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"✅ {OUTPUT}")
    print(f"   Packs totales:     {len(packs):>4}  ({len(TT_PACKS)} TT + {len(HASBRO_PACKS)} Hasbro)")
    print(f"   Con combo:         {stats['matched']:>4}")
    print(f"   Sin combo (sets):  {stats['unmatched']:>4}")
    print(f"   Relaciones piezas: {len(pack_parts):>4}")
    print(f"\n🔍 Próximos pasos:")
    print(f"   1. Abre scraped_packs.json y revisa que los part_ids son correctos")
    print(f"   2. Añade ASINs de Amazon a pack_affiliate_links")
    print(f"   3. Ejecuta insert_packs.py para subir a Supabase")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
