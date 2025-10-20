// data/batting.ts
import { readFileSync } from "node:fs";
import { parse } from "csv-parse/sync";

type RawRow = Record<string, string>;

export type BattingRow = {
  rk: number | null;
  player_raw: string;
  player_name: string; // cleaned (no *, #)
  bats: "L" | "R" | "S" | null;
  age: number | null;
  pos_raw: string;
  positions: string[];
  war: number | null;
  g: number | null;
  pa: number | null;
  ab: number | null;
  r: number | null;
  h: number | null;
  _2b: number | null;
  _3b: number | null;
  hr: number | null;
  rbi: number | null;
  sb: number | null;
  cs: number | null;
  bb: number | null;
  so: number | null;
  ba: number | null;
  obp: number | null;
  slg: number | null;
  ops: number | null;
  ops_plus: number | null; // from “OPS+”
  roba: number | null; // if this is actually wOBA, rename to woba
  rbat_plus: number | null; // from “Rbat+”
  tb: number | null;
  gidp: number | null;
  hbp: number | null;
  sh: number | null;
  sf: number | null;
  ibb: number | null;
  awards: string | null;
  player_id: string | null; // from “Player-additional”
  is_team_total: boolean;
};

const toNum = (v: string | undefined) => {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const toPct = (v: string | undefined) => {
  // handles .249 and 0.249 formats
  if (v == null || v === "") return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return n > 1 ? n : +n.toFixed(3); // keep decimals as given
};

const cleanPlayer = (raw: string) => {
  if (!raw) return { name: "", bats: null as BattingRow["bats"] };
  const bats: BattingRow["bats"] = raw.includes("*")
    ? "L"
    : raw.includes("#")
    ? "S"
    : null;
  const name = raw.replace("*", "").replace("#", "").trim();
  return { name, bats };
};

const splitPositions = (posRaw: string) =>
  posRaw
    ? posRaw
        .replace(/\*/g, "")
        .split(/[\/DH]/)
        .filter(Boolean)
    : [];

const keyMap: Record<string, string> = {
  Rk: "rk",
  Player: "player_raw",
  Age: "age",
  Pos: "pos_raw",
  WAR: "war",
  G: "g",
  PA: "pa",
  AB: "ab",
  R: "r",
  H: "h",
  "2B": "_2b",
  "3B": "_3b",
  HR: "hr",
  RBI: "rbi",
  SB: "sb",
  CS: "cs",
  BB: "bb",
  SO: "so",
  BA: "ba",
  OBP: "obp",
  SLG: "slg",
  OPS: "ops",
  "OPS+": "ops_plus",
  rOBA: "roba",
  "Rbat+": "rbat_plus",
  TB: "tb",
  GIDP: "gidp",
  HBP: "hbp",
  SH: "sh",
  SF: "sf",
  IBB: "ibb",
  Awards: "awards",
  "Player-additional": "player_id",
};

export function loadBattingCSV(path: string): BattingRow[] {
  const csv = readFileSync(path, "utf8");
  const rows: RawRow[] = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  });

  return rows
    .map((r) => {
      // remap keys to safe names
      const m: Record<string, string> = {};
      for (const [k, v] of Object.entries(r)) {
        const nk = keyMap[k] ?? k;
        m[nk] = v;
      }

      const isTeam = (m["player_raw"] ?? "")
        .toLowerCase()
        .includes("team totals");

      const { name: player_name, bats } = cleanPlayer(m["player_raw"] || "");
      const positions = splitPositions(m["pos_raw"] || "");

      const row: BattingRow = {
        rk: toNum(m["rk"]),
        player_raw: m["player_raw"] || "",
        player_name,
        bats,
        age: toNum(m["age"]),
        pos_raw: m["pos_raw"] || "",
        positions,
        war: toNum(m["war"]),
        g: toNum(m["g"]),
        pa: toNum(m["pa"]),
        ab: toNum(m["ab"]),
        r: toNum(m["r"]),
        h: toNum(m["h"]),
        _2b: toNum(m["_2b"]),
        _3b: toNum(m["_3b"]),
        hr: toNum(m["hr"]),
        rbi: toNum(m["rbi"]),
        sb: toNum(m["sb"]),
        cs: toNum(m["cs"]),
        bb: toNum(m["bb"]),
        so: toNum(m["so"]),
        ba: toPct(m["ba"]),
        obp: toPct(m["obp"]),
        slg: toPct(m["slg"]),
        ops: toPct(m["ops"]),
        ops_plus: toNum(m["ops_plus"]),
        roba: toPct(m["roba"]), // rename to woba if that’s your intent
        rbat_plus: toNum(m["rbat_plus"]),
        tb: toNum(m["tb"]),
        gidp: toNum(m["gidp"]),
        hbp: toNum(m["hbp"]),
        sh: toNum(m["sh"]),
        sf: toNum(m["sf"]),
        ibb: toNum(m["ibb"]),
        awards: m["awards"] || null,
        player_id:
          m["player_id"] && m["player_id"] !== "-9999" ? m["player_id"] : null,
        is_team_total: isTeam,
      };

      return row;
    })
    .filter((r) => !r.is_team_total); // drop Team Totals for player tools
}
