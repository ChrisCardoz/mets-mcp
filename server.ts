import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import { z } from "zod";
import { readFileSync } from "node:fs";
import { parse } from "csv-parse/sync";

/**
 * ---------------------------
 * Helpers: parsing & normalize
 * ---------------------------
 */
const toNum = (v?: string) => {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const toPct = (v?: string) => {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const cleanPlayer = (raw?: string) => {
  const s = (raw ?? "").trim();
  const bats = s.includes("*") ? "L" : s.includes("#") ? "S" : null;
  const name = s.replace("*", "").replace("#", "").trim();
  return { name, bats };
};

// Baseball IP like 168.2 means 168 and 2 outs (2/3 inning).
function ipToOuts(ipStr?: string): number | null {
  if (!ipStr) return null;
  const n = Number(ipStr);
  if (!Number.isFinite(n)) return null;
  const whole = Math.trunc(n);
  const frac = Number((n - whole).toFixed(1)); // .0 .1 .2 expected
  const outs = frac === 0.1 ? 1 : frac === 0.2 ? 2 : 0;
  return whole * 3 + outs;
}
const outsToInningsFloat = (outs: number | null) =>
  outs == null ? null : +(outs / 3).toFixed(3);

/**
 * ---------------------------
 * Batting loader (your headers)
 * ---------------------------
 * Rk,Player,Age,Pos,WAR,G,PA,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,BA,OBP,SLG,OPS,OPS+,rOBA,Rbat+,TB,GIDP,HBP,SH,SF,IBB,Pos,Awards,Player-additional
 */
type BattingRow = {
  rk: number | null;
  player_raw: string;
  player_name: string;
  bats: "L" | "R" | "S" | null;
  age: number | null;
  pos_raw: string | null;
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
  ops_plus: number | null; // OPS+
  roba: number | null; // your column is rOBA (keep as-is)
  rbat_plus: number | null; // Rbat+
  tb: number | null;
  gidp: number | null;
  hbp: number | null;
  sh: number | null;
  sf: number | null;
  ibb: number | null;
  awards: string | null;
  player_id: string | null; // Player-additional
};

const battingKeyMap: Record<string, string> = {
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

function loadBattingCSV(path: string): BattingRow[] {
  const csv = readFileSync(path, "utf8");
  const rows: Record<string, string>[] = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  });

  const out: BattingRow[] = [];
  for (const r of rows) {
    const m: Record<string, string> = {};
    for (const [k, v] of Object.entries(r)) {
      m[battingKeyMap[k] ?? k] = v;
    }
    const isTeam = (m["player_raw"] ?? "")
      .toLowerCase()
      .includes("team totals");
    if (isTeam) continue;

    const { name, bats } = cleanPlayer(m["player_raw"]);
    out.push({
      rk: toNum(m["rk"]),
      player_raw: m["player_raw"] ?? "",
      player_name: name,
      bats,
      age: toNum(m["age"]),
      pos_raw: m["pos_raw"] ?? null,
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
      roba: toPct(m["roba"]),
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
    });
  }
  return out;
}

/**
 * ----------------------------
 * Pitching loader (your headers)
 * ----------------------------
 * Rk,Player,Age,Pos,WAR,W,L,W-L%,ERA,G,GS,GF,CG,SHO,SV,IP,H,R,ER,HR,BB,IBB,SO,HBP,BK,WP,BF,ERA+,FIP,WHIP,H9,HR9,BB9,SO9,SO/BB,Awards,Player-additional
 */
type PitchingRow = {
  rk: number | null;
  player_raw: string;
  player_name: string;
  bats: "L" | "R" | "S" | null;
  age: number | null;
  pos_raw: string | null;
  war: number | null;
  w: number | null;
  l: number | null;
  wl_pct: number | null;
  era: number | null;
  g: number | null;
  gs: number | null;
  gf: number | null;
  cg: number | null;
  sho: number | null;
  sv: number | null;
  ip_outs: number | null; // canonical
  ip: number | null; // float
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
  so_per_bb: number | null;
  awards: string | null;
  player_id: string | null;
};

const pitchingKeyMap: Record<string, string> = {
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

function loadPitchingCSV(path: string): PitchingRow[] {
  const csv = readFileSync(path, "utf8");
  const rows: Record<string, string>[] = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  });

  const out: PitchingRow[] = [];
  for (const r of rows) {
    const m: Record<string, string> = {};
    for (const [k, v] of Object.entries(r)) {
      m[pitchingKeyMap[k] ?? k] = v;
    }
    const isTeam = (m["player_raw"] ?? "")
      .toLowerCase()
      .includes("team totals");
    if (isTeam) continue;

    const { name, bats } = cleanPlayer(m["player_raw"]);
    const ip_outs = ipToOuts(m["ip_csv"]);
    const ip = outsToInningsFloat(ip_outs);

    out.push({
      rk: toNum(m["rk"]),
      player_raw: m["player_raw"] ?? "",
      player_name: name,
      bats,
      age: toNum(m["age"]),
      pos_raw: m["pos_raw"] ?? null,
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
    });
  }
  return out;
}

/**
 * ---------------------------
 * Load both CSVs (normalized)
 * ---------------------------
 */
const batting = loadBattingCSV("./batting_2025_mets.csv");
const pitching = loadPitchingCSV("./pitching_2025_mets.csv");

/**
 * Metric/column aliases so your tools accept either original or normalized names.
 */
const metricAliases: Record<string, string> = {
  // batting
  "OPS+": "ops_plus",
  "ops+": "ops_plus",
  OPS_plus: "ops_plus",
  OPS: "ops",
  ops: "ops",
  BA: "ba",
  OBP: "obp",
  SLG: "slg",
  "Rbat+": "rbat_plus",
  "rbat+": "rbat_plus",
  "2B": "_2b",
  "3B": "_3b",
  SO: "so",
  BB: "bb",
  TB: "tb",
  rOBA: "roba",
  ROBA: "roba",
  // pitching
  "W-L%": "wl_pct",
  "w-l%": "wl_pct",
  "wl%": "wl_pct",
  "ERA+": "era_plus",
  "era+": "era_plus",
  "SO/BB": "so_per_bb",
  "K/BB": "so_per_bb",
  "k/bb": "so_per_bb",
  SO9: "so9",
  K9: "so9",
  BB9: "bb9",
  HR9: "hr9",
  H9: "h9",
  IP: "ip",
  ip_csv: "ip",
  WHIP: "whip",
  FIP: "fip",
  ERA: "era",
  BF: "bf",
};

const normalizeMetric = (m: string) =>
  metricAliases[m] ?? metricAliases[m.toUpperCase()] ?? m;
const normalizeColumns = (cols: string[]) =>
  cols.map((c) => normalizeMetric(c));

/**
 * ---------------------------
 * MCP Server + Tools
 * ---------------------------
 */
const server = new McpServer({ name: "mets-2025", version: "0.1.0" });

// Tool 1: single-player snapshot (batting or pitching)
server.registerTool(
  "get_player_stats",
  {
    title: "Get player stats",
    description:
      "Return a subset of batting or pitching columns for one player",
    inputSchema: {
      table: z.enum(["batting", "pitching"]),
      player: z.string(),
      columns: z.array(z.string()).min(1),
      filters: z.record(z.string(), z.string()).optional(), // keep for future use
    },
    outputSchema: z.object({ rows: z.array(z.record(z.string(), z.any())) }),
  },
  async ({ table, player, columns, filters }) => {
    const data = table === "batting" ? batting : pitching;
    const cols = normalizeColumns(columns);
    const rows = data
      .filter(
        (r: any) =>
          (r.player_name ?? r.Player ?? "").toLowerCase() ===
          player.toLowerCase()
      )
      .filter(
        (r: any) =>
          !filters ||
          Object.entries(filters).every(([k, v]) => String(r[k]) === v)
      )
      .map((r: any) => Object.fromEntries(cols.map((c) => [c, r[c]])));
    const output = { rows };
    return {
      content: [{ type: "text", text: JSON.stringify(output) }],
      structuredContent: output,
    };
  }
);

// Tool 2: simple leaderboard
server.registerTool(
  "leaderboard",
  {
    title: "Leaderboard",
    description: "Top-N by a metric with optional qualifier",
    inputSchema: {
      table: z.enum(["batting", "pitching"]),
      metric: z.string(), // e.g., "OPS+", "ops_plus", "FIP", "SO/BB"
      direction: z.enum(["asc", "desc"]),
      limit: z.number().min(1).max(25),
      qualifier: z
        .object({ minPA: z.number().optional(), minIP: z.number().optional() })
        .optional(),
    },
    outputSchema: z.object({ rows: z.array(z.record(z.string(), z.any())) }),
  },
  async ({ table, metric, direction, limit, qualifier }) => {
    const key = normalizeMetric(metric);
    const dataRaw = table === "batting" ? batting : pitching;

    // qualifiers: PA for batters, IP (float) for pitchers
    const data = dataRaw.filter((r: any) => {
      if (table === "batting" && qualifier?.minPA)
        return (r.pa ?? 0) >= qualifier.minPA;
      if (table === "pitching" && qualifier?.minIP)
        return (r.ip ?? 0) >= qualifier.minIP;
      return true;
    });

    const sorted = data
      .filter((r: any) => r[key] != null)
      .sort((a: any, b: any) => {
        const av = Number(
          a[key] ?? (direction === "asc" ? Infinity : -Infinity)
        );
        const bv = Number(
          b[key] ?? (direction === "asc" ? Infinity : -Infinity)
        );
        return direction === "asc" ? av - bv : bv - av;
      });

    const rows = sorted.slice(0, limit).map((r: any) => ({
      player: r.player_name,
      [key]: r[key],
      ...(table === "batting" ? { PA: r.pa } : { IP: r.ip }),
    }));

    const output = { rows };
    return {
      content: [{ type: "text", text: JSON.stringify(output) }],
      structuredContent: output,
    };
  }
);

// Minimal Streamable HTTP wiring
const app = express();
app.use(express.json());
app.post("/mcp", async (req, res) => {
  const transport = new StreamableHTTPServerTransport({
    enableJsonResponse: true,
  });
  res.on("close", () => transport.close());
  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});
app.listen(3000, () => console.log("MCP at http://localhost:3000/mcp"));
