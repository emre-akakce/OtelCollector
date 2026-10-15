// Alert forwarder sidecar.
// Receives OTLP/HTTP logs, picks records with attributes[alert]=true,
// then POSTs an Alertmanager-shaped payload to the notification API.

const http = require('http');

const PORT = Number(process.env.PORT || 4319);
const WEBHOOK_URL = process.env.WEBHOOK_URL
  || 'http://host.docker.internal:5252/notification/OpenTelemetry';

const EXTERNAL_URL = process.env.EXTERNAL_URL || 'http://otel-collector:4318';

// OTLP attribute values are shaped like {stringValue|boolValue|intValue|doubleValue}.
function readAttr(attrs, key) {
  if (!Array.isArray(attrs)) return undefined;
  const found = attrs.find((a) => a.key === key);
  if (!found || !found.value) return undefined;
  return (
    found.value.stringValue
    ?? found.value.boolValue
    ?? found.value.intValue
    ?? found.value.doubleValue
  );
}

function attrsToMap(attrs) {
  const out = {};
  if (!Array.isArray(attrs)) return out;
  for (const a of attrs) {
    const v =
      a.value?.stringValue
      ?? a.value?.boolValue
      ?? a.value?.intValue
      ?? a.value?.doubleValue;
    if (v !== undefined && v !== null) out[a.key] = String(v);
  }
  return out;
}

function toAlertmanagerPayload(record, resourceAttrs, scope) {
  const recAttrs = attrsToMap(record.attributes);
  const resAttrs = attrsToMap(resourceAttrs);

  const service = resAttrs['service.name'] || recAttrs['logger.category'] || 'unknown';
  const severity = (record.severityText || 'ERROR').toLowerCase();
  const body = typeof record.body === 'object'
    ? (record.body.stringValue ?? JSON.stringify(record.body))
    : String(record.body ?? '');

  const labels = {
    alertname: recAttrs['log.title'] || `${service}-${severity}`,
    severity,
    service,
    scope: scope || '',
  };
  const annotations = {
    summary: recAttrs['log.title'] || body,
    description: body,
    stacktrace: recAttrs['exception.stacktrace'] || recAttrs['log.stacktrace'] || '',
  };
  const commonLabels = {
    service,
    environment: resAttrs['deployment.environment'] || 'unknown',
    platform: resAttrs['platform'] || '',
  };

  return {
    receiver: 'alert-forwarder',
    status: 'firing',
    externalURL: EXTERNAL_URL,
    commonLabels,
    commonAnnotations: {},
    groupLabels: { alertname: labels.alertname },
    alerts: [{
      status: 'firing',
      labels,
      annotations,
      startsAt: new Date().toISOString(),
      fingerprint: `${service}:${labels.alertname}:${Date.now()}`,
    }],
  };
}

function postWebhook(payload) {
  return new Promise((resolve) => {
    const data = Buffer.from(JSON.stringify(payload));
    const url = new URL(WEBHOOK_URL);
    const req = http.request({
      method: 'POST',
      hostname: url.hostname,
      port: url.port || 80,
      path: url.pathname,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
      },
    }, (res) => {
      res.on('data', () => {});
      res.on('end', () => resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode }));
    });
    req.on('error', (err) => resolve({ ok: false, error: err.message }));
    req.write(data);
    req.end();
  });
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
      } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'POST' && req.url === '/v1/logs') {
    try {
      const body = await readJson(req);
      let fired = 0;
      let skipped = 0;

      for (const rl of body.resourceLogs || []) {
        const resourceAttrs = rl.resource?.attributes || [];
        for (const sl of rl.scopeLogs || []) {
          const scopeName = sl.scope?.name;
          for (const record of sl.logRecords || []) {
            const alertFlag = readAttr(record.attributes, 'alert');
            if (alertFlag === true || alertFlag === 'true') {
              const payload = toAlertmanagerPayload(record, resourceAttrs, scopeName);
              const result = await postWebhook(payload);
              if (!result.ok) {
                console.error('webhook failed:', result);
              } else {
                fired++;
              }
            } else {
              skipped++;
            }
          }
        }
      }

      console.log(`logs batch: fired=${fired} skipped=${skipped}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ partialSuccess: {} }));
    } catch (err) {
      console.error('bad OTLP request:', err);
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('bad request');
    }
    return;
  }

  // Accept traces/metrics too so collector doesn't error when exporting them.
  if (req.method === 'POST' && (req.url === '/v1/traces' || req.url === '/v1/metrics')) {
    req.on('data', () => {});
    req.on('end', () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{}');
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200);
    res.end('ok');
    return;
  }

  res.writeHead(404);
  res.end();
});

server.listen(PORT, () => {
  console.log(`alert-forwarder listening on :${PORT}, webhook=${WEBHOOK_URL}`);
});
