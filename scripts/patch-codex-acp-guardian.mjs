#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const bundlePath = resolve('node_modules/@agentclientprotocol/codex-acp/dist/index.js');
const helperPath = resolve('scripts/codex-acp-guardian-helpers.mjs');
const pristineSha256 = '3527bdaf90a219175c742576963e6d9e943e4ea5fbdbc3e04e7f57f9a9e11343';
const patchMarker = 'DIRECTOR_GUARDIAN_DENIAL_EXTENSION_V1';
const extensionMethod = '_director/approve_guardian_denied_action';

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function replaceExactlyOnce(source, search, replacement, label) {
  const first = source.indexOf(search);
  if (first === -1 || source.indexOf(search, first + search.length) !== -1) {
    throw new Error(`codex-acp anchor mismatch: ${label}`);
  }
  return source.slice(0, first) + replacement + source.slice(first + search.length);
}

function helperSource() {
  const source = readFileSync(helperPath, 'utf8');
  return source
    .replace("import { createHash } from 'node:crypto';\n\n", '')
    .replaceAll('export ', '');
}

function patchedBundle(pristine) {
  let source = pristine;
  source = replaceExactlyOnce(
    source,
    'var ASYNC_TASK_STOP_METHOD = "_session/async_task/stop";\n',
    `var ASYNC_TASK_STOP_METHOD = "_session/async_task/stop";\nvar DIRECTOR_APPROVE_GUARDIAN_DENIED_ACTION_METHOD = "${extensionMethod}";\n`,
    'extension method constant'
  );
  source = replaceExactlyOnce(
    source,
    'return request.method === "authentication/status" || request.method === "authentication/logout" || request.method === LEGACY_SET_SESSION_MODEL_METHOD || request.method === GOAL_CONTROL_METHOD || request.method === LEGACY_GOAL_CONTROL_METHOD || request.method === SESSION_STEERING_METHOD || request.method === ASYNC_TASK_STOP_METHOD;',
    'return request.method === "authentication/status" || request.method === "authentication/logout" || request.method === LEGACY_SET_SESSION_MODEL_METHOD || request.method === GOAL_CONTROL_METHOD || request.method === LEGACY_GOAL_CONTROL_METHOD || request.method === SESSION_STEERING_METHOD || request.method === ASYNC_TASK_STOP_METHOD || request.method === DIRECTOR_APPROVE_GUARDIAN_DENIED_ACTION_METHOD;',
    'extension allowlist'
  );
  source = replaceExactlyOnce(
    source,
    '  async threadStart(params) {\n    return await this.sendRequest({ method: "thread/start", params });\n  }\n',
    '  async threadApproveGuardianDeniedAction(params) {\n    return await this.sendRequest({ method: "thread/approveGuardianDeniedAction", params });\n  }\n  async threadStart(params) {\n    return await this.sendRequest({ method: "thread/start", params });\n  }\n',
    'native approval method'
  );
  const guardianUpdate = 'function createGuardianApprovalReviewToolCallUpdate(event) {\n  return {\n    sessionUpdate: "tool_call_update",\n    toolCallId: guardianApprovalReviewToolCallId(event.reviewId),\n    status: toAcpGuardianApprovalReviewStatus(event.review.status),\n    content: createGuardianApprovalReviewContent(event.review, event.action),\n    rawOutput: event\n  };\n}\n';
  const replacement = `${helperSource()}\nvar directorGuardianStore = new GuardianDenialStore();\nfunction createGuardianApprovalReviewToolCallUpdate(event) {\n  const candidate = directorGuardianStore.captureCompleted(event);\n  return {\n    sessionUpdate: "tool_call_update",\n    toolCallId: guardianApprovalReviewToolCallId(event.reviewId),\n    status: toAcpGuardianApprovalReviewStatus(event.review.status),\n    content: createGuardianApprovalReviewContent(event.review, event.action),\n    rawOutput: candidate ? { directorApproval: candidate } : event\n  };\n}\n`;
  source = replaceExactlyOnce(source, guardianUpdate, replacement, 'guardian update');
  source = replaceExactlyOnce(
    source,
    '      if (isTurnCompletedNotification(serverNotification)) {\n',
    '      if (serverNotification.method === "item/started") {\n        directorGuardianStore.captureStarted(serverNotification.params.threadId, serverNotification.params.item);\n      }\n      if (isTurnCompletedNotification(serverNotification)) {\n',
    'Director item capture'
  );
  source = replaceExactlyOnce(
    source,
    '      case GOAL_CONTROL_METHOD:\n',
    `      case DIRECTOR_APPROVE_GUARDIAN_DENIED_ACTION_METHOD: {\n        const params = methodRequest.params;\n        try {\n          return await directorGuardianStore.approve(params, (nativeRequest) =>\n            this.codexAcpClient.appServerClient.threadApproveGuardianDeniedAction(nativeRequest)\n          );\n        } catch (error) {\n          throw RequestError.invalidParams(void 0, error instanceof Error ? error.message : "unknown Director guardian approval");\n        }\n      }\n      case GOAL_CONTROL_METHOD:\n`,
    'extension dispatch'
  );
  source = replaceExactlyOnce(
    source,
    'var asyncTaskStopParamsParser = external_exports.object({\n',
    'var directorGuardianApprovalParamsParser = external_exports.object({ sessionId: external_exports.string(), reviewId: external_exports.string(), fingerprint: external_exports.string().regex(/^[0-9a-f]{64}$/) }).strict();\nvar asyncTaskStopParamsParser = external_exports.object({\n',
    'extension parameters'
  );
  source = replaceExactlyOnce(
    source,
    '.onRequest(GOAL_CONTROL_METHOD, goalControlParamsParser, (ctx) => getAgent().extMethod(GOAL_CONTROL_METHOD, ctx.params)).connect(acpJsonStream);',
    '.onRequest(GOAL_CONTROL_METHOD, goalControlParamsParser, (ctx) => getAgent().extMethod(GOAL_CONTROL_METHOD, ctx.params)).onRequest(DIRECTOR_APPROVE_GUARDIAN_DENIED_ACTION_METHOD, directorGuardianApprovalParamsParser, (ctx) => getAgent().extMethod(DIRECTOR_APPROVE_GUARDIAN_DENIED_ACTION_METHOD, ctx.params)).connect(acpJsonStream);',
    'extension route'
  );
  return `${source}\n// ${patchMarker}\n`;
}

const current = readFileSync(bundlePath, 'utf8');
if (current.includes(patchMarker)) {
  const expectedPatchedSha256 = 'a9871f4e133e55f821d84f7246cf1d5d7d7d096dfb9a5c03b00beb0d65bcf6db';
  if (sha256(current) !== expectedPatchedSha256) {
    throw new Error('unexpected patched codex-acp bundle hash');
  }
  process.exit(0);
}
if (sha256(current) !== pristineSha256) {
  throw new Error('unexpected pristine codex-acp bundle hash');
}
const patched = patchedBundle(current);
writeFileSync(bundlePath, patched);
console.log(sha256(patched));
