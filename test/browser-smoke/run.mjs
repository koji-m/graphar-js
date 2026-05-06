import { createReadStream } from 'node:fs';
import { access, constants } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const distEntry = path.join(rootDir, 'dist', 'graphar.es.js');

const contentTypes = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.yml': 'text/yaml; charset=utf-8',
  '.yaml': 'text/yaml; charset=utf-8',
};

function contentTypeFor(filePath) {
  return contentTypes[path.extname(filePath)] ?? 'application/octet-stream';
}

function createStaticServer(root) {
  return http.createServer(async (request, response) => {
    try {
      const requestUrl = new URL(request.url ?? '/', 'http://127.0.0.1');
      const pathname = decodeURIComponent(requestUrl.pathname);
      const relativePath =
        pathname === '/' ? 'test/browser-smoke/index.html' : pathname.replace(/^\/+/, '');
      const filePath = path.join(root, relativePath);
      await access(filePath, constants.R_OK);
      response.writeHead(200, {
        'content-type': contentTypeFor(filePath),
        'cache-control': 'no-store',
      });
      createReadStream(filePath).pipe(response);
    } catch {
      response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      response.end('not found');
    }
  });
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Failed to determine browser smoke server address.');
  }
  return `http://127.0.0.1:${address.port}/`;
}

async function closeServer(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function main() {
  await access(distEntry, constants.R_OK);

  const server = createStaticServer(rootDir);
  const baseUrl = await listen(server);
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const consoleMessages = [];
  const pageErrors = [];

  page.on('console', (message) => {
    consoleMessages.push(`${message.type()}: ${message.text()}`);
  });
  page.on('pageerror', (error) => {
    pageErrors.push(error.stack || error.message);
  });

  try {
    await page.goto(new URL('test/browser-smoke/index.html', baseUrl).href, {
      waitUntil: 'networkidle',
    });
    try {
      await page.waitForFunction(() => Boolean(globalThis.__GRAPHAR_BROWSER_SMOKE__), {
        timeout: 30000,
      });
    } catch (error) {
      const statusText = await page.locator('#status').textContent().catch(() => null);
      throw new Error(
        [
          error instanceof Error ? error.message : String(error),
          statusText ? `status text:\n${statusText}` : '',
          pageErrors.length > 0 ? `page errors:\n${pageErrors.join('\n')}` : '',
          consoleMessages.length > 0
            ? `console messages:\n${consoleMessages.join('\n')}`
            : '',
        ]
          .filter(Boolean)
          .join('\n\n'),
      );
    }
    const result = await page.evaluate(() => globalThis.__GRAPHAR_BROWSER_SMOKE__);
    if (!result?.ok) {
      throw new Error(
        [
          `Browser smoke failed: ${result?.message ?? 'unknown error'}`,
          result?.stack ?? '',
          pageErrors.length > 0 ? `page errors:\n${pageErrors.join('\n')}` : '',
          consoleMessages.length > 0
            ? `console messages:\n${consoleMessages.join('\n')}`
            : '',
        ]
          .filter(Boolean)
          .join('\n\n'),
      );
    }

    if (!result.exports?.hasGraphInfo || !result.exports?.hasEdgesCollection) {
      throw new Error(`Browser smoke exported surface is incomplete: ${JSON.stringify(result)}`);
    }

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await browser.close();
    await closeServer(server);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
