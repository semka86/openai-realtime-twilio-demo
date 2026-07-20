import { RawData, WebSocket } from "ws";
import functions from "./functionHandlers";

interface TranscriptEntry {
  role: "caller" | "assistant";
  text: string;
}

interface Session {
  twilioConn?: WebSocket;
  frontendConn?: WebSocket;
  modelConn?: WebSocket;
  streamSid?: string;
  callSid?: string;
  callerNumber?: string;
  saved_config?: any;
  lastAssistantItem?: string;
  responseStartTimestamp?: number;
  latestMediaTimestamp?: number;
  openAIApiKey?: string;
  transcript?: TranscriptEntry[];
  callStartedAt?: string;
  emailSent?: boolean;
}

let session: Session = {};

export function handleCallConnection(
  ws: WebSocket,
  openAIApiKey: string
) {
  cleanupConnection(session.twilioConn);
  cleanupConnection(session.modelConn);

  session.twilioConn = ws;
  session.modelConn = undefined;
  session.openAIApiKey = openAIApiKey;
  session.streamSid = undefined;
  session.callSid = undefined;
  session.callerNumber = undefined;
  session.transcript = [];
  session.callStartedAt = new Date().toISOString();
  session.emailSent = false;

  ws.on("message", handleTwilioMessage);

  ws.on("error", (error) => {
    console.error("Twilio WebSocket error:", error);
    ws.close();
  });

  ws.on("close", () => {
    finishCurrentCall();

    const modelConnection = session.modelConn;
    session.twilioConn = undefined;
    session.modelConn = undefined;

    cleanupConnection(modelConnection);
    resetCallData();

    if (!session.frontendConn) {
      session = {};
    }
  });
}

export function handleFrontendConnection(ws: WebSocket) {
  cleanupConnection(session.frontendConn);
  session.frontendConn = ws;

  ws.on("message", handleFrontendMessage);

  ws.on("close", () => {
    session.frontendConn = undefined;

    if (!session.twilioConn && !session.modelConn) {
      session = {};
    }
  });
}

async function handleFunctionCall(item: {
  name: string;
  arguments: string;
}) {
  console.log("Handling function call:", item.name);

  const fnDef = functions.find(
    (func) => func.schema.name === item.name
  );

  if (!fnDef) {
    throw new Error(
      `No handler found for function: ${item.name}`
    );
  }

  let args: unknown;

  try {
    args = JSON.parse(item.arguments);
  } catch {
    return JSON.stringify({
      error: "Invalid JSON arguments for function call.",
    });
  }

  try {
    const result = await fnDef.handler(args as any);

    return typeof result === "string"
      ? result
      : JSON.stringify(result);
  } catch (error: any) {
    console.error("Error running function:", error);

    return JSON.stringify({
      error: `Error running function ${item.name}: ${
        error?.message || "Unknown error"
      }`,
    });
  }
}

function handleTwilioMessage(data: RawData) {
  const msg = parseMessage(data);
  if (!msg) return;

  switch (msg.event) {
    case "start": {
      session.streamSid = msg.start?.streamSid;
      session.callSid = msg.start?.callSid;

      const customParameters =
        msg.start?.customParameters || {};

      session.callerNumber =
        customParameters.from ||
        customParameters.From ||
        customParameters.caller ||
        customParameters.Caller ||
        undefined;

      session.latestMediaTimestamp = 0;
      session.lastAssistantItem = undefined;
      session.responseStartTimestamp = undefined;
      session.transcript = [];
      session.callStartedAt = new Date().toISOString();
      session.emailSent = false;

      console.log("CALL DEBUG:", {
        customParameters,
        callerNumber: session.callerNumber || null,
      });

      console.log("Twilio call started:", {
        streamSid: session.streamSid,
        callSid: session.callSid,
        callerNumber: session.callerNumber,
      });

      tryConnectModel();
      break;
    }

    case "media": {
      session.latestMediaTimestamp = Number(
        msg.media?.timestamp || 0
      );

      if (
        isOpen(session.modelConn) &&
        typeof msg.media?.payload === "string"
      ) {
        jsonSend(session.modelConn, {
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        });
      }

      break;
    }

    case "stop":
    case "close": {
      finishCurrentCall();
      closeCallConnections();
      break;
    }
  }
}

function handleFrontendMessage(data: RawData) {
  const msg = parseMessage(data);
  if (!msg) return;

  if (msg.type === "session.update") {
    session.saved_config = msg.session;
    return;
  }

  if (isOpen(session.modelConn)) {
    jsonSend(session.modelConn, msg);
  }
}

function tryConnectModel() {
  if (
    !session.twilioConn ||
    !session.streamSid ||
    !session.openAIApiKey
  ) {
    return;
  }

  if (isOpen(session.modelConn)) {
    return;
  }

  const modelConnection = new WebSocket(
    "wss://api.openai.com/v1/realtime?model=gpt-realtime",
    {
      headers: {
        Authorization: `Bearer ${session.openAIApiKey}`,
      },
    }
  );

  session.modelConn = modelConnection;

  modelConnection.on("open", () => {
    const config = session.saved_config || {};

    jsonSend(modelConnection, {
      type: "session.update",
      session: {
        ...config,

        type: "realtime",
        model: "gpt-realtime",
        output_modalities: ["audio"],

instructions: `
אתה נציג טלפוני של חברת לומינור – ניהול ואחזקת מבנים.

ענה בעברית בלבד.

פתח תמיד ב:
"שלום, הגעתם ללומינור. איך אפשר לעזור?"

אסוף רק את שלושת הפרטים הבאים:
1. שם פרטי.
2. מהות הפנייה.
3. העיר שבה נדרש השירות.

אם אחד מהפרטים כבר נאמר, אל תשאל עליו שוב.

אל תשאל על:
- מספר טלפון
- כתובת
- מספר בניין
- דירה
- שעות נוחות
- כל פרט אחר

לאחר שיש בידך את שלושת הפרטים, אמור:

"תודה רבה. קיבלתי את הפרטים. נציג מטעמנו יחזור אליך בהקדם."

לאחר מכן סיים את השיחה מיד.

אם הלקוח מוסיף מידע נוסף מיוזמתו, קבל אותו, אך אל תשאל שאלות המשך.
`,
        audio: {
          input: {
            format: {
              type: "audio/pcmu",
            },

            noise_reduction: {
              type: "near_field",
            },

            transcription: {
              model: "gpt-4o-transcribe",
              language: "he",
              prompt:
                "Keywords: לומינור, ניהול ואחזקת מבנים, ועד בית, מצלמות אבטחה, אינטרקום, תקלה, הצעת מחיר, בניין, כניסה, דירה, אשקלון, שדרות, קריית גת, נחל לכיש",
            },

            turn_detection: {
              type: "server_vad",
              threshold: 0.55,
              prefix_padding_ms: 500,
              silence_duration_ms: 900,
              create_response: true,
              interrupt_response: true,
            },
          },

          output: {
            format: {
              type: "audio/pcmu",
            },
            voice: "marin",
          },
        },
      },
    });
  });

  modelConnection.on("message", handleModelMessage);

  modelConnection.on("error", (error) => {
    console.error(
      "OpenAI Realtime WebSocket error:",
      error
    );
  });

  modelConnection.on("close", () => {
    if (session.modelConn === modelConnection) {
      session.modelConn = undefined;
    }

    if (!session.twilioConn && !session.frontendConn) {
      session = {};
    }
  });
}

function handleModelMessage(data: RawData) {
  const event = parseMessage(data);
  if (!event) return;

  jsonSend(session.frontendConn, event);

  switch (event.type) {
    case "input_audio_buffer.speech_started": {
      handleTruncation();
      break;
    }

    case "conversation.item.input_audio_transcription.completed": {
      const text = cleanTranscriptText(
        event.transcript
      );

      if (text) {
        addTranscriptEntry("caller", text);
        console.log("Caller transcript:", text);
      }

      break;
    }

    case "conversation.item.input_audio_transcription.failed": {
      console.error(
        "Caller transcription failed:",
        JSON.stringify(event.error || event)
      );
      break;
    }

    case "response.output_audio_transcript.done": {
  const text = cleanTranscriptText(event.transcript);

  if (text) {
    addTranscriptEntry("assistant", text);

    console.log(
      "Assistant transcript:",
      text
    );

    if (
      text.includes("נציג מטעמנו יחזור אליך בהקדם")
    ) {
      hangupCallAfterDelay(7000);
    }
  }

  break;
}

    case "response.output_text.done": {
      const text = cleanTranscriptText(event.text);

      if (text) {
        addTranscriptEntry("assistant", text);
      }

      break;
    }

    case "response.output_audio.delta": {
      if (
        isOpen(session.twilioConn) &&
        session.streamSid &&
        typeof event.delta === "string"
      ) {
        if (
          session.responseStartTimestamp === undefined
        ) {
          session.responseStartTimestamp =
            session.latestMediaTimestamp || 0;
        }

        if (typeof event.item_id === "string") {
          session.lastAssistantItem =
            event.item_id;
        }

        jsonSend(session.twilioConn, {
          event: "media",
          streamSid: session.streamSid,
          media: {
            payload: event.delta,
          },
        });

        jsonSend(session.twilioConn, {
          event: "mark",
          streamSid: session.streamSid,
          mark: {
            name: "assistant-response",
          },
        });
      }

      break;
    }

    case "response.output_item.done": {
      const item = event.item;

      if (
        item?.type === "function_call" &&
        typeof item.name === "string" &&
        typeof item.arguments === "string"
      ) {
        void handleFunctionCall({
          name: item.name,
          arguments: item.arguments,
        })
          .then((output) => {
            if (!isOpen(session.modelConn)) {
              return;
            }

            jsonSend(session.modelConn, {
              type: "conversation.item.create",
              item: {
                type: "function_call_output",
                call_id: item.call_id,
                output,
              },
            });

            jsonSend(session.modelConn, {
              type: "response.create",
            });
          })
          .catch((error) => {
            console.error(
              "Error handling function call:",
              error
            );
          });
      }

      break;
    }

    case "error": {
      console.error(
        "OpenAI Realtime error:",
        JSON.stringify(event)
      );
      break;
    }
  }
}

function addTranscriptEntry(
  role: "caller" | "assistant",
  text: string
) {
  const cleanedText = cleanTranscriptText(text);
  if (!cleanedText) return;

  if (!session.transcript) {
    session.transcript = [];
  }

  const previousEntry =
    session.transcript[
      session.transcript.length - 1
    ];

  if (
    previousEntry &&
    previousEntry.role === role &&
    previousEntry.text === cleanedText
  ) {
    return;
  }

  session.transcript.push({
    role,
    text: cleanedText,
  });
}

function cleanTranscriptText(
  value: unknown
): string {
  if (typeof value !== "string") {
    return "";
  }

  return value.replace(/\s+/g, " ").trim();
}

function finishCurrentCall() {
  if (session.emailSent) {
    return;
  }

  session.emailSent = true;

  const finishedSession: Session = {
    ...session,
    transcript: [
      ...(session.transcript || []),
    ],
    emailSent: false,
  };

  void finalizeCall(finishedSession).catch(
    (error) => {
      console.error(
        "Failed to finalize call:",
        error
      );
    }
  );
}

async function finalizeCall(
  callSession: Session
) {
  if (callSession.emailSent) {
    return;
  }

  callSession.emailSent = true;

  const transcript =
    callSession.transcript || [];

  if (transcript.length === 0) {
    console.log(
      "No transcript collected; email will not be sent."
    );
    return;
  }

  const transcriptText =
    formatTranscript(transcript);

  let summary =
    "לא ניתן היה ליצור סיכום אוטומטי.";

  try {
    summary = await createCallSummary(
      transcriptText,
      callSession.openAIApiKey
    );
  } catch (error) {
    console.error(
      "Failed creating call summary:",
      error
    );
  }

  try {
    await sendLeadEmail({
      summary,
      transcript: transcriptText,
      callerNumber:
        callSession.callerNumber,
      callSid: callSession.callSid,
      startedAt:
        callSession.callStartedAt,
    });

    console.log(
      "Lead email sent successfully."
    );
  } catch (error) {
    console.error(
      "Failed sending lead email:",
      error
    );
  }
}

function formatTranscript(
  entries: TranscriptEntry[]
): string {
  return entries
    .map((entry) => {
      const speaker =
        entry.role === "caller"
          ? "המתקשר"
          : "נציג לומינור";

      return `${speaker}: ${entry.text}`;
    })
    .join("\n\n");
}

async function createCallSummary(
  transcript: string,
  apiKey?: string
): Promise<string> {
  if (!apiKey) {
    throw new Error(
      "OPENAI_API_KEY is missing"
    );
  }

  const response = await fetch(
    "https://api.openai.com/v1/responses",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },

      body: JSON.stringify({
        model: "gpt-5-mini",

        input: [
          {
            role: "system",
            content:
              "אתה מסכם שיחות שירות עבור חברת לומינור לניהול ואחזקת מבנים. כתוב בעברית ברורה ותמציתית. אין להמציא מידע שלא נאמר. כאשר יש חזרות או תיקונים בשיחה, השתמש בפרט האחרון שהמתקשר אישר.",
          },
          {
            role: "user",
            content: `סכם את השיחה הבאה.

הצג את הסיכום במבנה הבא:

שם המתקשר:
כתובת הבניין או מקום השירות:
נושא הפנייה:
פירוט הבקשה:
דחיפות:
פעולה מומלצת לנציג:
פרטים חסרים:

כאשר פרט לא נאמר או לא אושר בבירור, כתוב "לא נמסר".

תמלול השיחה:
${transcript}`,
          },
        ],
      }),
    }
  );

  const body: any = await response.json();

  if (!response.ok) {
    throw new Error(
      `OpenAI summary error ${
        response.status
      }: ${JSON.stringify(body)}`
    );
  }

  if (
    typeof body.output_text === "string" &&
    body.output_text.trim()
  ) {
    return body.output_text.trim();
  }

  const extractedText =
    extractResponseText(body);

  if (!extractedText) {
    throw new Error(
      "OpenAI returned no summary text"
    );
  }

  return extractedText;
}

function extractResponseText(body: any): string {
  const texts: string[] = [];

  if (!Array.isArray(body?.output)) {
    return "";
  }

  for (const outputItem of body.output) {
    if (
      !Array.isArray(outputItem?.content)
    ) {
      continue;
    }

    for (const contentItem of outputItem.content) {
      if (
        typeof contentItem?.text ===
          "string" &&
        contentItem.text.trim()
      ) {
        texts.push(
          contentItem.text.trim()
        );
      }
    }
  }

  return texts.join("\n").trim();
}

async function sendLeadEmail(details: {
  summary: string;
  transcript: string;
  callerNumber?: string;
  callSid?: string;
  startedAt?: string;
}) {
  const resendApiKey =
    process.env.RESEND_API_KEY;

  const leadsEmail =
    process.env.LEADS_EMAIL;

  if (!resendApiKey) {
    throw new Error(
      "RESEND_API_KEY is missing"
    );
  }

  if (!leadsEmail) {
    throw new Error(
      "LEADS_EMAIL is missing"
    );
  }

  const callDate = details.startedAt
    ? new Date(
        details.startedAt
      ).toLocaleString("he-IL", {
        timeZone: "Asia/Jerusalem",
      })
    : new Date().toLocaleString("he-IL", {
        timeZone: "Asia/Jerusalem",
      });

  const callerNumber =
    details.callerNumber ||
    "לא התקבל אוטומטית";

  const subjectCaller =
    details.callerNumber
      ? ` - ${details.callerNumber}`
      : "";

  const html = `
<div dir="rtl" style="font-family:Arial,sans-serif;line-height:1.6;max-width:800px;margin:auto">
  <h1 style="font-size:22px">פנייה חדשה – לומינור</h1>

  <p>
    <strong>מועד השיחה:</strong>
    ${escapeHtml(callDate)}
  </p>

  <p>
    <strong>מספר המתקשר:</strong>
    <span dir="ltr">${escapeHtml(callerNumber)}</span>
  </p>

  <p>
    <strong>מזהה שיחה:</strong>
    <span dir="ltr">${escapeHtml(
      details.callSid || "לא התקבל"
    )}</span>
  </p>

  <hr>

  <h2 style="font-size:18px">
    סיכום השיחה
  </h2>

  <div style="white-space:pre-wrap;background:#f5f5f5;padding:15px;border-radius:8px">
${escapeHtml(details.summary)}
  </div>

  <h2 style="font-size:18px;margin-top:25px">
    תמלול מלא
  </h2>

  <div style="white-space:pre-wrap;background:#f5f5f5;padding:15px;border-radius:8px">
${escapeHtml(details.transcript)}
  </div>
</div>`;

  const text = `פנייה חדשה – לומינור

מועד השיחה: ${callDate}
מספר המתקשר: ${callerNumber}
מזהה שיחה: ${
    details.callSid || "לא התקבל"
  }

סיכום:
${details.summary}

תמלול מלא:
${details.transcript}`;

  const response = await fetch(
    "https://api.resend.com/emails",
    {
      method: "POST",

      headers: {
        Authorization:
          `Bearer ${resendApiKey}`,
        "Content-Type": "application/json",
      },

      body: JSON.stringify({
        from:
          "Luminor <onboarding@resend.dev>",
        to: [leadsEmail],
        subject:
          `פנייה חדשה מהנציג האוטומטי${subjectCaller}`,
        html,
        text,
      }),
    }
  );

  const body: any = await response.json();

  if (!response.ok) {
    throw new Error(
      `Resend error ${
        response.status
      }: ${JSON.stringify(body)}`
    );
  }

  console.log("Resend response:", body);
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function handleTruncation() {
  if (
    !session.lastAssistantItem ||
    session.responseStartTimestamp ===
      undefined
  ) {
    return;
  }

  const elapsedMs =
    (session.latestMediaTimestamp || 0) -
    session.responseStartTimestamp;

  const audioEndMs =
    Math.max(0, elapsedMs);

  if (isOpen(session.modelConn)) {
    jsonSend(session.modelConn, {
      type: "conversation.item.truncate",
      item_id: session.lastAssistantItem,
      content_index: 0,
      audio_end_ms: audioEndMs,
    });
  }

  if (
    isOpen(session.twilioConn) &&
    session.streamSid
  ) {
    jsonSend(session.twilioConn, {
      event: "clear",
      streamSid: session.streamSid,
    });
  }

  session.lastAssistantItem = undefined;
  session.responseStartTimestamp =
    undefined;
}

function closeCallConnections() {
  const twilioConnection =
    session.twilioConn;

  const modelConnection =
    session.modelConn;

  session.twilioConn = undefined;
  session.modelConn = undefined;

  cleanupConnection(modelConnection);
  cleanupConnection(twilioConnection);

  resetCallData();

  if (!session.frontendConn) {
    session = {};
  }
}

function resetCallData() {
  session.streamSid = undefined;
  session.callSid = undefined;
  session.callerNumber = undefined;
  session.lastAssistantItem = undefined;
  session.responseStartTimestamp =
    undefined;
  session.latestMediaTimestamp =
    undefined;
  session.transcript = undefined;
  session.callStartedAt = undefined;
}
function hangupCallAfterDelay(delayMs = 7000) {
  if (!session.twilioConn) {
    return;
  }

  setTimeout(() => {
    if (isOpen(session.twilioConn)) {
      console.log("Disconnecting call...");
      session.twilioConn.close();
    }
  }, delayMs);
}
function cleanupConnection(ws?: WebSocket) {
  if (isOpen(ws)) {
    ws.close();
  }
}

function parseMessage(data: RawData): any {
  try {
    return JSON.parse(data.toString());
  } catch {
    return null;
  }
}

function jsonSend(
  ws: WebSocket | undefined,
  obj: unknown
) {
  if (!isOpen(ws)) {
    return;
  }

  ws.send(JSON.stringify(obj));
}

function isOpen(
  ws?: WebSocket
): ws is WebSocket {
  return (
    !!ws &&
    ws.readyState === WebSocket.OPEN
  );
}