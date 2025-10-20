import { MCPServerStreamableHttp } from "@openai/agents";
const s = new MCPServerStreamableHttp({ url: "https://…/mcp" });
await s.connect();
console.log((await s.listTools()).map((t) => t.name));
