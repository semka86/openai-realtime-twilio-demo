import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import { IncomingMessage } from "http";
import dotenv from "dotenv";
import http from "http";
import { readFileSync } from "fs";
import { join } from "path";
import cors from "cors";
import {
  handleCallConnection,
  handleFrontendConnection,
} from "./sessionManager";
import functions from "./functionHandlers";

dotenv.config();

const PORT = parseInt(process.env.PORT || "8081", 10);
const PUBLIC_URL = process.env.PUBLIC_URL || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";

if (!OPENAI_API_KEY) {
  console.error("OPENAI_API_KEY environment variable is required");
  process.exit(1);
}

if (!PUBLIC_URL) {
  console.error("PUBLIC_URL environment variable is required");
  process.exit(1);
}

const app = express();

app.use(cors());

// Twilio שולח את נתוני השיחה כ-form-urlencoded
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const twimlPath = join(__dirname, "twiml.xml");
const twimlTemplate = readFileSync(twimlPath, "utf-8");

app.get("/public-url", (_req, res) => {
  res.json({ publicUrl: PUBLIC_URL });
});

app.all("/twiml", (req, res) => {
  try {
    const wsUrl = new URL(PUBLIC_URL);
    wsUrl.protocol = "wss:";
    wsUrl.pathname = "/call";
    wsUrl.search = "";

    const from =
      typeof req.body?.From === "string"
        ? req.body.From
        : typeof req.query?.From === "string"
          ? req.query.From
          : "";

    const callSid =
      typeof req.body?.CallSid === "string"
        ? req.body.CallSid
        : typeof req.query?.CallSid === "string"
          ? req.query.CallSid
          : "";

    console.log("Incoming Twilio call:", {
      from,
      callSid,
      method: req.method,
    });

    const twimlContent = twimlTemplate
      .replace("{{WS_URL}}", escapeXml(wsUrl.toString()))
      .replace("{{From}}", escapeXml(from))
      .replace("{{CallSid}}", escapeXml(callSid));

    res.type("text/xml").send(twimlContent);
  } catch (error) {
    console.error("Failed generating TwiML:", error);

    res.status(500).type("text/xml").send(
      `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say language="he-IL">אירעה שגיאה זמנית. אנא נסו שוב מאוחר יותר.</Say>
</Response>`
    );
  }
});

// רשימת הכלים הזמינים
app.get("/tools", (_req, res) => {
  res.json(functions.map((f) => f.schema));
});

let currentCall: WebSocket | null = null;
let currentLogs: WebSocket | null = null;

wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
  const url = new URL(
    req.url || "",
    `http://${req.headers.host || "localhost"}`
  );

  const parts = url.pathname.split("/").filter(Boolean);

  if (parts.length < 1) {
    ws.close();
    return;
  }

  const type = parts[0];

  if (type === "call") {
    if (currentCall && currentCall.readyState === WebSocket.OPEN) {
      currentCall.close();
    }

    currentCall = ws;
    handleCallConnection(currentCall, OPENAI_API_KEY);

    ws.on("close", () => {
      if (currentCall === ws) {
        currentCall = null;
      }
    });
  } else if (type === "logs") {
    if (currentLogs && currentLogs.readyState === WebSocket.OPEN) {
      currentLogs.close();
    }

    currentLogs = ws;
    handleFrontendConnection(currentLogs);

    ws.on("close", () => {
      if (currentLogs === ws) {
        currentLogs = null;
      }
    });
  } else {
    ws.close();
  }
});

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});