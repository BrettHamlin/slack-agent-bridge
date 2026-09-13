import { createHash } from 'node:crypto';

export const DIRECTOR_SERVER = 'director-publish-reply';
export const DIRECTOR_TOOL = 'publish_reply';
const MAX_RECORDS = 128;

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function jsonWithPythonAscii(value) {
  return JSON.stringify(value).replace(/[\u007f-\uffff]/g, (character) =>
    `\\u${character.charCodeAt(0).toString(16).padStart(4, '0')}`
  );
}

function sortedObject(value) {
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, value[key]])
  );
}

export function canonicalReplyPayload(argumentsValue) {
  const payload = {
    execution_fence: argumentsValue.execution_fence ?? null,
    responsibility_id: argumentsValue.responsibility_id ?? null,
    text: argumentsValue.text
  };
  const hasConversationMetadata = [
    argumentsValue.conversation_title,
    argumentsValue.conversation_emoji,
    argumentsValue.conversation_preview
  ].some((value) => value != null);
  if (hasConversationMetadata) {
    payload.conversation_emoji = argumentsValue.conversation_emoji ?? null;
    payload.conversation_preview = argumentsValue.conversation_preview ?? null;
    payload.conversation_title = argumentsValue.conversation_title ?? null;
  }
  return sortedObject(payload);
}

export function directorPayloadDigest(argumentsValue) {
  return sha256(jsonWithPythonAscii(canonicalReplyPayload(argumentsValue)));
}

function eventKey(sessionId, reviewId) {
  return `${sessionId}\u0000${reviewId}`;
}

function itemKey(sessionId, itemId) {
  return `${sessionId}\u0000${itemId}`;
}

function isExactDirectorCall(item) {
  const argumentsValue = item?.arguments;
  return (
    item?.type === 'mcpToolCall' &&
    item?.server === DIRECTOR_SERVER &&
    item?.tool === DIRECTOR_TOOL &&
    typeof item?.id === 'string' &&
    typeof argumentsValue?.authority === 'string' &&
    typeof argumentsValue?.text === 'string'
  );
}

function toNativeGuardianEvent(event) {
  const nativeEvent = {
    id: event.reviewId,
    target_item_id: event.targetItemId,
    turn_id: event.turnId,
    started_at_ms: event.startedAtMs,
    completed_at_ms: event.completedAtMs,
    status: 'denied',
    risk_level: event.review.riskLevel,
    user_authorization: event.review.userAuthorization,
    rationale: event.review.rationale,
    decision_source: event.decisionSource,
    action: {
      type: 'mcp_tool_call',
      server: event.action.server,
      tool_name: event.action.toolName,
      connector_id: event.action.connectorId,
      connector_name: event.action.connectorName,
      tool_title: event.action.toolTitle
    }
  };
  for (const object of [nativeEvent, nativeEvent.action]) {
    for (const key of Object.keys(object)) {
      if (object[key] === undefined) {
        delete object[key];
      }
    }
  }
  return nativeEvent;
}

function isExactDeniedDirectorReview(event, item) {
  return (
    event?.review?.status === 'denied' &&
    event?.action?.type === 'mcpToolCall' &&
    event.action.server === DIRECTOR_SERVER &&
    event.action.toolName === DIRECTOR_TOOL &&
    isExactDirectorCall(item) &&
    event.targetItemId === item.id
  );
}

export class GuardianDenialStore {
  constructor({ now = () => Date.now(), ttlMs = 600_000 } = {}) {
    this.now = now;
    this.ttlMs = ttlMs;
    this.items = new Map();
    this.records = new Map();
    this.consumed = new Map();
  }

  cleanup() {
    const minimumTime = this.now() - this.ttlMs;
    for (const collection of [this.items, this.records, this.consumed]) {
      for (const [key, value] of collection) {
        if (value.createdAt < minimumTime) {
          collection.delete(key);
        }
      }
    }
    for (const collection of [this.items, this.records, this.consumed]) {
      while (collection.size > MAX_RECORDS) {
        collection.delete(collection.keys().next().value);
      }
    }
  }

  captureStarted(threadId, item) {
    this.cleanup();
    if (!isExactDirectorCall(item)) {
      return false;
    }
    this.items.set(itemKey(threadId, item.id), {
      createdAt: this.now(),
      item
    });
    return true;
  }

  captureCompleted(event) {
    this.cleanup();
    const key = eventKey(event?.threadId, event?.reviewId);
    const storedItem = this.items.get(itemKey(event?.threadId, event?.targetItemId));
    this.items.delete(itemKey(event?.threadId, event?.targetItemId));
    if (this.consumed.has(key)) {
      return null;
    }
    const existing = this.records.get(key);
    if (existing) {
      return existing.candidate;
    }
    const item = storedItem?.item;
    if (!isExactDeniedDirectorReview(event, item)) {
      return null;
    }
    const nativeEvent = toNativeGuardianEvent(event);
    const fingerprint = sha256(JSON.stringify({ event: nativeEvent, arguments: item.arguments }));
    const candidate = {
      sessionId: event.threadId,
      turnId: event.turnId,
      reviewId: event.reviewId,
      fingerprint,
      authority: item.arguments.authority,
      payloadDigest: directorPayloadDigest(item.arguments),
      replyDisplay: {
        text: item.arguments.text,
        conversation_title: item.arguments.conversation_title ?? null,
        conversation_emoji: item.arguments.conversation_emoji ?? null,
        conversation_preview: item.arguments.conversation_preview ?? null
      }
    };
    this.records.set(key, {
      createdAt: this.now(),
      candidate,
      nativeEvent
    });
    return candidate;
  }

  async approve({ sessionId, reviewId, fingerprint }, nativeApprove) {
    this.cleanup();
    const key = eventKey(sessionId, reviewId);
    const record = this.records.get(key);
    if (!record || record.candidate.fingerprint !== fingerprint) {
      throw new Error('unknown Director guardian approval');
    }
    this.records.delete(key);
    this.consumed.set(key, { createdAt: this.now() });
    await nativeApprove({ threadId: sessionId, event: record.nativeEvent });
    return { approved: true };
  }
}
