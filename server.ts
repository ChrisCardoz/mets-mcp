import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import { z } from "zod";
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";
import { parse } from "csv-parse/sync";

/** Helpers */
const toNum = (v?: string) =>
  v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null;
const toPct = (v?: string) =>
  v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null;
const cleanPlayer = (raw?: string) => {
  const s = (raw ?? "").trim();
  const bats = s.includes("*") ? "L" : s.includes("#") ? "S" : null;
  const name = s.replace("*", "").replace("#", "").trim();
  return { name, bats };
};
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

/** Batting loader */
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
  ops_plus: number | null;
  roba: number | null;
  rbat_plus: number | null;
  tb: number | null;
  gidp: number | null;
  hbp: number | null;
  sh: number | null;
  sf: number | null;
  ibb: number | null;
  awards: string | null;
  player_id: string | null;
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
    for (const [k, v] of Object.entries(r)) m[battingKeyMap[k] ?? k] = v;
    if ((m["player_raw"] ?? "").toLowerCase().includes("team totals")) continue;
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

/** Pitching loader */
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
  ip_outs: number | null;
  ip: number | null;
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
    for (const [k, v] of Object.entries(r)) m[pitchingKeyMap[k] ?? k] = v;
    if ((m["player_raw"] ?? "").toLowerCase().includes("team totals")) continue;
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

/** Load data (multi-team, 2025) */
const TEAMS = [
  "ARI",
  "ATL",
  "BAL",
  "BOS",
  "CHC",
  "CIN",
  "CLE",
  "COL",
  "CWS",
  "DET",
  "HOU",
  "KC",
  "LAA",
  "LAD",
  "MIA",
  "MIL",
  "MIN",
  "NYM",
  "NYY",
  "OAK",
  "PHI",
  "PIT",
  "SD",
  "SEA",
  "SF",
  "STL",
  "TB",
  "TEX",
  "TOR",
  "WSH",
] as const;
type TeamCode = (typeof TEAMS)[number];

type BattingWithTeam = BattingRow & { team: TeamCode };
type PitchingWithTeam = PitchingRow & { team: TeamCode };

const battingByTeam: Record<string, BattingWithTeam[]> = {};
const pitchingByTeam: Record<string, PitchingWithTeam[]> = {};

function loadSeasonTeams(season: string) {
  const base = join("data", season);
  if (!existsSync(base)) return;
  for (const entry of readdirSync(base)) {
    const teamDir = join(base, entry);
    let isDir = false;
    try {
      isDir = statSync(teamDir).isDirectory();
    } catch {
      isDir = false;
    }
    if (!isDir) continue;

    const team = entry.toUpperCase() as TeamCode;

    try {
      const b = loadBattingCSV(join(teamDir, "batting.csv")).map(
        (r) => ({ ...(r as BattingRow), team })
      );
      battingByTeam[team] = b;
    } catch {
      // ignore missing batting.csv for team
    }

    try {
      const p = loadPitchingCSV(join(teamDir, "pitching.csv")).map(
        (r) => ({ ...(r as PitchingRow), team })
      );
      pitchingByTeam[team] = p;
    } catch {
      // ignore missing pitching.csv for team
    }
  }
}

// Initialize by scanning data/2025/*
loadSeasonTeams("2025");

/** Metric aliases */
const metricAliases: Record<string, string> = {
  // batting
  "OPS+": "ops_plus",
  "ops+": "ops_plus",
  OPS_plus: "ops_plus",
  OPS: "ops",
  ops: "ops",
  // Map unavailable FanGraphs-style metrics to available equivalents
  // Note: these are proxies, not identical formulas.
  "wRC+": "rbat_plus",
  "WRC+": "rbat_plus",
  "wrc+": "rbat_plus",
  fWAR: "war",
  FWAR: "war",
  fwar: "war",
  // WAR synonyms and natural language intents
  WAR: "war",
  war: "war",
  "wins above replacement": "war",
  "the best": "war",
  best: "war",
  "most valuable": "war",
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
  // Map unavailable FanGraphs-style ERA- to ERA+ as a rough substitute
  "ERA-": "era_plus",
  "era-": "era_plus",
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
// Natural-language phrases we intentionally interpret as WAR
const NATURAL_LANGUAGE_ALIASES = new Set<string>([
  "the best",
  "best",
  "most valuable",
  "wins above replacement",
]);

type NormalizedMetric = { key: string; note?: string };
const normalizeMetricDetailed = (m: string): NormalizedMetric => {
  const raw = String(m ?? "");
  const key = metricAliases[raw] ?? metricAliases[raw.toUpperCase()] ?? raw;
  const isNatural = NATURAL_LANGUAGE_ALIASES.has(raw.trim().toLowerCase());
  return isNatural && key !== raw
    ? { key, note: `interpreted '${raw}' as '${key.toUpperCase() === key ? key : key}'` }
    : { key };
};

const normalizeMetric = (m: string) => normalizeMetricDetailed(m).key;
const normalizeColumns = (cols: string[]) => cols.map((c) => normalizeMetric(c));

const normalizeColumnsWithNotes = (cols: string[]) => {
  const keys: string[] = [];
  const notes: string[] = [];
  for (const c of cols) {
    const { key, note } = normalizeMetricDetailed(c);
    keys.push(key);
    if (note) notes.push(note);
  }
  return { keys, notes };
};

// --- Position helpers (batting) ---
const POS_MAP: Record<string, string> = {
  "1": "P",
  "2": "C",
  "3": "1B",
  "4": "2B",
  "5": "3B",
  "6": "SS",
  "7": "LF",
  "8": "CF",
  "9": "RF",
  D: "DH",
};
function extractPositions(posRaw?: string | null): string[] {
  if (!posRaw) return [];
  // Example inputs: "*4/DH3", "5H/46D", "/473DH9", "UT"
  const rawU = (posRaw || "").toUpperCase();
  const tokens = rawU.replace(/\*/g, "").split(/[\/]/).filter(Boolean);
  const out = new Set<string>();
  if (rawU.includes("UT")) out.add("UT");
  for (const t of tokens) {
    for (const ch of t) {
      const mapped = POS_MAP[ch];
      if (mapped) out.add(mapped);
    }
  }
  return Array.from(out);
}
function normalizePositionQuery(
  q?: string | null
): { targets: Set<string>; isOutfield: boolean; isUtility: boolean } {
  if (!q) return { targets: new Set(), isOutfield: false };
  const s = q.trim().toLowerCase();
  const map: Record<string, string> = {
    "c": "C",
    "catcher": "C",
    "1b": "1B",
    "first": "1B",
    "first base": "1B",
    "2b": "2B",
    "second": "2B",
    "second base": "2B",
    "3b": "3B",
    "third": "3B",
    "third base": "3B",
    "ss": "SS",
    "short": "SS",
    "shortstop": "SS",
    "lf": "LF",
    "left": "LF",
    "left field": "LF",
    "cf": "CF",
    "center": "CF",
    "center field": "CF",
    "rf": "RF",
    "right": "RF",
    "right field": "RF",
    "of": "OF",
    "outfield": "OF",
    "if": "INF",
    "inf": "INF",
    "infield": "INF",
    "dh": "DH",
    "designated hitter": "DH",
    "ut": "UT",
    "util": "UT",
    "utility": "UT",
    "utility player": "UT",
    "utility players": "UT",
    "utility infielder": "INF",
  };
  const lookup = map[s] || map[s.replace(/\s+/g, " ")] || map[s.replace(/\s+/g, "")] || s.toUpperCase();
  if (lookup === "OF") return { targets: new Set(["LF", "CF", "RF"]), isOutfield: true, isUtility: false };
  if (lookup === "INF") return { targets: new Set(["1B", "2B", "3B", "SS"]), isOutfield: false, isUtility: false };
  if (lookup === "UT") return { targets: new Set(["UT"]), isOutfield: false, isUtility: true };
  return { targets: new Set([lookup]), isOutfield: false, isUtility: false };
}

/** MCP server + tools */
const server = new McpServer({ name: "mlb-2025", version: "0.3.0" });

// Tool: single-player snapshot
server.registerTool(
  "get_player_stats",
  {
    description:
      "Return a subset of batting or pitching columns for one player (MLB 2025 dataset)",
    inputSchema: {
      table: z.enum(["batting", "pitching"]),
      scope: z.enum(["team", "league"]).default("team"),
      team: z.string().length(3).default("NYM"),
      player: z.string(),
      columns: z.array(z.string()).min(1),
      filters: z.record(z.string(), z.string()).optional(),
    },
  },
  async ({ table, scope, team, player, columns, filters }) => {
    const dataMap = table === "batting" ? battingByTeam : pitchingByTeam;
    const data =
      scope === "league"
        ? Object.values(dataMap).flat()
        : dataMap[String(team).toUpperCase()] ?? [];
    const { keys: cols, notes } = normalizeColumnsWithNotes(columns);
    const rows = data
      .filter(
        (r: any) => (r.player_name ?? "").toLowerCase() === player.toLowerCase()
      )
      .filter(
        (r: any) =>
          !filters ||
          Object.entries(filters).every(([k, v]) => String(r[k]) === v)
      )
      .map((r: any) => {
        const base = Object.fromEntries(cols.map((c) => [c, r[c]]));
        return scope === "league" ? { team: r.team, ...base } : base;
      });
    const output = notes.length ? { rows, warnings: Array.from(new Set(notes)) } : { rows };
    return {
      content: [{ type: "text", text: JSON.stringify(output) }],
      structuredContent: output,
    };
  }
);

// Tool: leaderboard
server.registerTool(
  "leaderboard",
  {
    description: "Top-N by a metric with optional qualifier (MLB 2025 dataset)",
    inputSchema: {
      table: z.enum(["batting", "pitching"]),
      team: z.string().length(3).default("NYM"),
      scope: z.enum(["team", "league"]).default("league"),
      metric: z.string(), // e.g., "OPS+", "ops_plus", "FIP", "SO/BB"
      direction: z.enum(["asc", "desc"]),
      limit: z.number().min(1).max(25),
      qualifier: z
        .object({ minPA: z.number().optional(), minIP: z.number().optional() })
        .optional(),
      // New: optional position filter (batting only). Accepts aliases like "2B", "second base", "OF".
      position: z.string().optional(),
    },
  },
  async ({ table, team, scope, metric, direction, limit, qualifier, position }) => {
    const { key, note } = normalizeMetricDetailed(metric);
    const map = table === "batting" ? battingByTeam : pitchingByTeam;
    const dataRaw =
      scope === "league"
        ? Object.values(map).flat()
        : map[String(team).toUpperCase()] ?? [];

    const dataPre = dataRaw.filter((r: any) => {
      if (table === "batting" && qualifier?.minPA)
        return (r.pa ?? 0) >= qualifier.minPA;
      if (table === "pitching" && qualifier?.minIP)
        return (r.ip ?? 0) >= qualifier.minIP;
      return true;
    });

    // Optional: filter by position for batting
    const data =
      table === "batting" && position
        ? dataPre.filter((r: any) => {
            const { targets, isUtility } = normalizePositionQuery(position);
            const posList = extractPositions(r.pos_raw);
            if (isUtility) {
              const upper = String(r.pos_raw || "").toUpperCase();
              const isUT = upper.includes("UT");
              const infield = new Set(["1B", "2B", "3B", "SS"]);
              const infieldCount = posList.filter((p) => infield.has(p)).length;
              const isINFUtility = infieldCount >= 2; // rotational infielder
              return isUT || isINFUtility;
            }
            if (targets.size === 0) return true;
            if (!posList.length) return false;
            return posList.some((p) => targets.has(p));
          })
        : dataPre;

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
      team: r.team,
      player: r.player_name,
      [key]: r[key],
      ...(table === "batting" ? { PA: r.pa } : { IP: r.ip }),
      ...(table === "batting" ? { pos: extractPositions(r.pos_raw).join("/") } : {}),
    }));

    const warnings = note ? [note] : [];
    const output = warnings.length ? { rows, warnings } : { rows };
    return {
      content: [{ type: "text", text: JSON.stringify(output) }],
      structuredContent: output,
    };
  }
);

// Tool: teams listing
server.registerTool(
  "teams",
  {
    description: "List teams discovered under data/2025",
    inputSchema: {},
  },
  async () => {
    const uniq = new Set<string>();
    Object.keys(battingByTeam).forEach((t) => uniq.add(t));
    Object.keys(pitchingByTeam).forEach((t) => uniq.add(t));
    const teams = Array.from(uniq).sort();
    const output = { teams };
    return {
      content: [{ type: "text", text: JSON.stringify(output) }],
      structuredContent: output,
    };
  }
);

/** Express wiring (order matters) */
const app = express();
app.use(express.json());

// CORS / preflight
app.options("/mcp", (req, res) => {
  res.set({
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
  });
  res.sendStatus(204);
});

// Health check (GET /mcp)
app.get("/mcp", (req, res) => {
  res.set({ "Access-Control-Allow-Origin": "*" });
  res
    .status(200)
    .json({ ok: true, server: "mlb-2025", transport: "streamable-http" });
});

// Accept header patch before POST
app.use("/mcp", (req, _res, next) => {
  const acc = String(req.headers["accept"] || "");
  const hasJSON = acc.includes("application/json");
  const hasSSE = acc.includes("text/event-stream");
  if (!hasJSON || !hasSSE)
    req.headers["accept"] = "application/json, text/event-stream";
  next();
});

// Single POST handler
app.post("/mcp", async (req, res) => {
  res.set({
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  });

  const transport = new StreamableHTTPServerTransport({
    enableJsonResponse: true,
  });
  res.on("close", () => transport.close());
  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});

app.listen(3000, () => console.log("MCP at http://localhost:3000/mcp"));
