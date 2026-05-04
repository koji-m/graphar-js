import * as arrow from 'apache-arrow';
import initWasm, { readParquet } from 'parquet-wasm/esm';
import { HttpClient } from './http-client.js';

class HttpFileSystem {
  constructor(client) {
    this.client = client;
  }

  async readFileAsText(path) {
    const response = await this.client.get(path);
    return await response.text();
  }

  async readFileAsSingleUint64(path) {
    const response = await this.client.get(path);
    const buffer = await response.arrayBuffer();
    if (buffer.byteLength < 8) {
      throw new Error(`File size must be 8 byte+: ${path}`);
    }
    const view = new DataView(buffer);

    return view.getBigUint64(0, true);
  }

  async readFileAsTable(path, _fileType, columns) {
    const response = await this.client.get(path);
    const dataUint8Array = new Uint8Array(await response.arrayBuffer());
    const options = {
      columns,
    };
    // TODO: branch fileType
    const arrowWasmTable = readParquet(dataUint8Array, options);
    const arrowTable = arrow.tableFromIPC(arrowWasmTable.intoIPCStream());

    return arrowTable;
  }
}

async function readLocalFile(path) {
  try {
    const { readFile } = await import('node:fs/promises');
    return await readFile(path);
  } catch (error) {
    throw new Error(
      `Local filesystem access is only supported in Node.js: ${path}`,
      { cause: error },
    );
  }
}

class LocalFileSystem {
  async readFileAsText(path) {
    const buffer = await readLocalFile(path);
    return buffer.toString('utf8');
  }

  async readFileAsSingleUint64(path) {
    const buffer = await readLocalFile(path);
    if (buffer.byteLength < 8) {
      throw new Error(`File size must be 8 byte+: ${path}`);
    }
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

    return view.getBigUint64(0, true);
  }

  async readFileAsTable(path, _fileType, columns) {
    const dataUint8Array = new Uint8Array(await readLocalFile(path));
    const options = {
      columns,
    };
    // TODO: branch fileType
    const arrowWasmTable = readParquet(dataUint8Array, options);
    const arrowTable = arrow.tableFromIPC(arrowWasmTable.intoIPCStream());

    return arrowTable;
  }
}

function isAbsoluteLocalPath(path) {
  return path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path);
}

function isFileUrl(path) {
  return path.startsWith('file://');
}

function fileSystemFromUriOrPath(baseUri) {
  if (baseUri.length < 1) {
    throw new Error('Base URI or path must be provided.');
  }
  if (baseUri.startsWith('http://') || baseUri.startsWith('https://')) {
    return [new HttpFileSystem(new HttpClient()), baseUri];
  }
  if (isAbsoluteLocalPath(baseUri)) {
    return [new LocalFileSystem(), baseUri];
  }
  if (isFileUrl(baseUri)) {
    const url = new URL(baseUri);
    if (url.host.length > 0 && url.host !== 'localhost') {
      throw new Error(`Unsupported file URI host: ${baseUri}`);
    }
    return [new LocalFileSystem(), decodeURIComponent(url.pathname)];
  }
  throw new Error(
    'Unsupported URI scheme or relative local path. Use http://, https://, file://, or an absolute path.',
  );
}

export { fileSystemFromUriOrPath, initWasm };
