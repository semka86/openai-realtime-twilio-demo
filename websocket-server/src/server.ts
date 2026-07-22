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
// זיכרון זמני לשיחות WhatsApp.
// בשלב ראשון נשמר בזיכרון השרת בלבד.
const whatsappConversations = new Map<
  string,
  Array<{ role: "user" | "assistant"; content: string }>
>();

app.post("/whatsapp", async (req, res) => {
  const from = typeof req.body?.From === "string" ? req.body.From : "";
  const message = typeof req.body?.Body === "string"
    ? req.body.Body.trim()
    : "";

  console.log("Incoming WhatsApp message:", {
    from,
    message,
  });

  if (!from || !message) {
    res.type("text/xml").send(
      `<?xml version="1.0" encoding="UTF-8"?>
<Response></Response>`
    );
    return;
  }

  try {
    const previousMessages = whatsappConversations.get(from) || [];

    // שומרים רק את 20 ההודעות האחרונות כדי שהשיחה לא תהיה כבדה מדי.
    const conversation = [
      ...previousMessages,
      {
        role: "user" as const,
        content: message,
      },
    ].slice(-20);

    const instructions = `
אתה עונה ללקוחות בוואטסאפ בשם העסק לומינור ובסגנון הכתיבה של סיימון, בעל העסק.

תחומי הפעילות:
מצלמות אבטחה, אינטרקום, בקרת כניסה, אזעקות, תקשורת,
ניהול ואחזקת מבנים.

כללי שיחה:
- ענה בצורה טבעית, קצרה, מקצועית ונעימה.
- אל תשתמש בתפריטים ממוספרים אלא אם הם באמת נחוצים.
- שאל בכל הודעה לכל היותר שאלה אחת או שתיים.
- ענה באותה שפה שבה הלקוח כתב: עברית, רוסית או אנגלית.
- אל תמציא מחירים, זמני הגעה, מלאי או התחייבויות.
- אל תאשר עבודה או תור בלי אישור מפורש מסיימון.
- כאשר חסר מידע, בקש אותו באופן טבעי.
- עבור הצעת מחיר, נסה להבין מה נדרש, באיזה סוג מקום, באיזו עיר,
  כמה יחידות או מצלמות נדרשות והאם קיימת תשתית.
- עבור תקלה, בקש תיאור קצר, דגם אם ידוע ותמונה או סרטון כשזה יעזור.
- אם הלקוח מבקש לדבר עם סיימון, אמור: "בשמחה, ראיתי את ההודעה ואחזור אליך כשאתפנה".
- אל תכתוב שהודעה הועברה, נשלחה או טופלה אם לא בוצעה פעולה כזאת בפועל.
- אל תכתוב שאתה GPT ואל תשתמש בשפה רובוטית.
- אל תטען שביצעת פעולה בעולם האמיתי אם לא ביצעת אותה.
- אתה סיימון עצמו, ולא נציג מטעם סיימון.
- אל תציג את עצמך מחדש בכל הודעה.
- בהודעה הראשונה אפשר לכתוב "היי, מה נשמע?" ולאחר מכן להמשיך ישר לעניין.
- השתמש בניסוח יומיומי ופשוט, בלי משפטי שירות רשמיים כמו "כיצד אוכל לסייע".
- כאשר לקוח אומר שמישהו נתן לו את המספר, שאל באופן טבעי: "בשמחה, במה מדובר?"
`;

    const openAIResponse = await fetch(
      "https://api.openai.com/v1/responses",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: "gpt-5-mini",
          instructions,
          input: conversation.map((item) => ({
            role: item.role,
            content: [
              {
                type:
                  item.role === "assistant"
                    ? "output_text"
                    : "input_text",
                text: item.content,
              },
            ],
          })),
          max_output_tokens: 350,
        }),
      }
    );

    if (!openAIResponse.ok) {
      const errorText = await openAIResponse.text();

      console.error("OpenAI WhatsApp error:", {
        status: openAIResponse.status,
        errorText,
      });

      throw new Error(`OpenAI returned ${openAIResponse.status}`);
    }

    const result: any = await openAIResponse.json();

    const reply =
      typeof result.output_text === "string"
        ? result.output_text.trim()
        : result.output
            ?.flatMap((item: any) => item.content || [])
            ?.find((item: any) => item.type === "output_text")
            ?.text?.trim();

    if (!reply) {
      throw new Error("OpenAI returned an empty WhatsApp reply");
    }

    whatsappConversations.set(
  from,
  [
    ...conversation,
    {
      role: "assistant" as const,
      content: reply,
    },
  ].slice(-20)
);

    res.type("text/xml").send(
      `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Message>${escapeXml(reply)}</Message>
</Response>`
    );
  } catch (error) {
    console.error("WhatsApp webhook failed:", error);

    res.type("text/xml").send(
      `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Message>${escapeXml(
    "קיבלתי את ההודעה. כרגע יש תקלה זמנית במענה ואחזור אליך בהקדם."
  )}</Message>
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