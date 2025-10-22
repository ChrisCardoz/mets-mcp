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

/** MCP server + tools */
const server = new McpServer({ name: "mlb-2025", version: "0.2.0" });

// Tool: single-player snapshot
server.registerTool(
  "get_player_stats",
  {
    description:
      "Return a subset of batting or pitching columns for one player",
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
    const cols = normalizeColumns(columns);
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
    const output = { rows };
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
    description: "Top-N by a metric with optional qualifier",
    inputSchema: {
      table: z.enum(["batting", "pitching"]),
      team: z.string().length(3).default("NYM"),
      scope: z.enum(["team", "league"]).default("team"),
      metric: z.string(), // e.g., "OPS+", "ops_plus", "FIP", "SO/BB"
      direction: z.enum(["asc", "desc"]),
      limit: z.number().min(1).max(25),
      qualifier: z
        .object({ minPA: z.number().optional(), minIP: z.number().optional() })
        .optional(),
    },
  },
  async ({ table, team, scope, metric, direction, limit, qualifier }) => {
    const key = normalizeMetric(metric);
    const map = table === "batting" ? battingByTeam : pitchingByTeam;
    const dataRaw =
      scope === "league"
        ? Object.values(map).flat()
        : map[String(team).toUpperCase()] ?? [];

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
      team: r.team,
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
