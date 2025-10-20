// data/pitching.ts
import { readFileSync } from "node:fs";
import { parse } from "csv-parse/sync";

type Raw = Record<string, string>;

export type PitchingRow = {
  rk: number | null;
  player_raw: string;
  player_name: string;
  bats: "L" | "R" | "S" | null; // inferred from * (L) / # (S), else null
  age: number | null;
  pos_raw: string | null;
  war: number | null;
  w: number | null;
  l: number | null;
  wl_pct: number | null; // W-L%
  era: number | null;
  g: number | null;
  gs: number | null;
  gf: number | null;
  cg: number | null;
  sho: number | null;
  sv: number | null;
  ip_outs: number | null; // total outs (canonical for math)
  ip: number | null; // true-innings float, e.g., 168.667
  h: number | null;
  r: number | null;
  er: number | null;
  hr: number | null;
  bb: number | null;
  ibb: number | null;
  so: number | null;
  hbp: number | null;
  bk: number | null;
  wp: number | null;
  bf: number | null;
  era_plus: number | null;
  fip: number | null;
  whip: number | null;
  h9: number | null;
  hr9: number | null;
  bb9: number | null;
  so9: number | null;
  so_per_bb: number | null; // from SO/BB
  awards: string | null;
  player_id: string | null; // from Player-additional
  is_team_total: boolean;
};

const toNum = (v?: string) => {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const toPct = (v?: string) => {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null; // values are already decimals (e.g., .600) in your CSV
};

// Convert baseball IP notation to outs & real innings.
// e.g., 168.2 -> 168 innings, ".2" means 2 outs = 2/3 inning.
function ipToOuts(ipStr?: string): number | null {
  if (!ipStr) return null;
  const n = Number(ipStr);
  if (!Number.isFinite(n)) return null;
  const whole = Math.trunc(n);
  const frac = Number((n - whole).toFixed(1)); // .0, .1, .2 only
  if (![0, 0.1, 0.2].includes(frac)) {
    // Fallback: treat any other decimal as nearest third
    const outs = Math.round((n - whole) * 3);
    return whole * 3 + outs;
  }
  const outs = frac === 0.1 ? 1 : frac === 0.2 ? 2 : 0;
  return whole * 3 + outs;
}
const outsToInningsFloat = (outs: number | null) =>
  outs == null ? null : +(outs / 3).toFixed(3);

const cleanPlayer = (raw: string) => {
  const bats: PitchingRow["bats"] = raw?.includes("*")
    ? "L"
    : raw?.includes("#")
    ? "S"
    : null;
  const name = raw?.replace("*", "").replace("#", "").trim() ?? "";
  return { name, bats };
};

const keyMap: Record<string, string> = {
  Rk: "rk",
  Player: "player_raw",
  Age: "age",
  Pos: "pos_raw",
  WAR: "war",
  W: "w",
  L: "l",
  "W-L%": "wl_pct",
  ERA: "era",
  G: "g",
  GS: "gs",
  GF: "gf",
  CG: "cg",
  SHO: "sho",
  SV: "sv",
  IP: "ip_csv",
  H: "h",
  R: "r",
  ER: "er",
  HR: "hr",
  BB: "bb",
  IBB: "ibb",
  SO: "so",
  HBP: "hbp",
  BK: "bk",
  WP: "wp",
  BF: "bf",
  "ERA+": "era_plus",
  FIP: "fip",
  WHIP: "whip",
  H9: "h9",
  HR9: "hr9",
  BB9: "bb9",
  SO9: "so9",
  "SO/BB": "so_per_bb",
  Awards: "awards",
  "Player-additional": "player_id",
};

export function loadPitchingCSV(path: string): PitchingRow[] {
  const csv = readFileSync(path, "utf8");
  const rows: Raw[] = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  });

  return rows
    .map((r) => {
      // remap headers to safe keys
      const m: Record<string, string> = {};
      for (const [k, v] of Object.entries(r)) {
        const nk = keyMap[k] ?? k;
        m[nk] = v;
      }

      const isTeam = (m["player_raw"] ?? "")
        .toLowerCase()
        .includes("team totals");
      const { name: player_name, bats } = cleanPlayer(m["player_raw"] || "");
      const ip_outs = ipToOuts(m["ip_csv"]);
      const ip = outsToInningsFloat(ip_outs);

      const row: PitchingRow = {
        rk: toNum(m["rk"]),
        player_raw: m["player_raw"] || "",
        player_name,
        bats,
        age: toNum(m["age"]),
        pos_raw: m["pos_raw"] || null,
        war: toNum(m["war"]),
        w: toNum(m["w"]),
        l: toNum(m["l"]),
        wl_pct: toPct(m["wl_pct"]),
        era: toNum(m["era"]),
        g: toNum(m["g"]),
        gs: toNum(m["gs"]),
        gf: toNum(m["gf"]),
        cg: toNum(m["cg"]),
        sho: toNum(m["sho"]),
        sv: toNum(m["sv"]),
        ip_outs,
        ip,
        h: toNum(m["h"]),
        r: toNum(m["r"]),
        er: toNum(m["er"]),
        hr: toNum(m["hr"]),
        bb: toNum(m["bb"]),
        ibb: toNum(m["ibb"]),
        so: toNum(m["so"]),
        hbp: toNum(m["hbp"]),
        bk: toNum(m["bk"]),
        wp: toNum(m["wp"]),
        bf: toNum(m["bf"]),
        era_plus: toNum(m["era_plus"]),
        fip: toNum(m["fip"]),
        whip: toNum(m["whip"]),
        h9: toNum(m["h9"]),
        hr9: toNum(m["hr9"]),
        bb9: toNum(m["bb9"]),
        so9: toNum(m["so9"]),
        so_per_bb: toNum(m["so_per_bb"]),
        awards: m["awards"] || null,
        player_id:
          m["player_id"] && m["player_id"] !== "-9999" ? m["player_id"] : null,
        is_team_total: isTeam,
      };

      return row;
    })
    .filter((r) => !r.is_team_total);
}
