// Tiny webhook sink for the smoke test: records requests to a file, can be told to fail N times.
const http = require('http');
const fs = require('fs');

const out = process.argv[2];
const port = Number(process.argv[3]);
let failNext = 0;

http
  .createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      if (req.url.startsWith('/__fail')) {
        failNext = Number(new URL(req.url, 'http://x').searchParams.get('n') || 0);
        res.end('ok');
        return;
      }
      const status = failNext > 0 ? 500 : 200;
      if (failNext > 0) failNext--;
      fs.appendFileSync(
        out,
        JSON.stringify({ status, at: Date.now(), headers: req.headers, body: JSON.parse(raw || '{}') }) + '\n',
      );
      res.statusCode = status;
      res.end('{}');
    });
  })
  .listen(port, '127.0.0.1');
