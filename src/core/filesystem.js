import * as arrow from 'apache-arrow';
import initWasm, { readParquet } from 'parquet-wasm';
import { HttpClient } from './http-client.js';

class FileSystem {
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

function fileSystemFromUriOrPath(baseUri) {
  if (baseUri.length < 1) {
    throw new Error('Base URI or path must be provided.');
  }
  if (baseUri.startsWith('/')) {
    throw new Error(
      'URI starting with "/" is not supported in this FileSystem implementation.',
    );
  }
  if (baseUri.startsWith('http://') || baseUri.startsWith('https://')) {
    return [new FileSystem(new HttpClient()), baseUri];
  }
  throw new Error('Unsupported URI scheme.');
}

export { fileSystemFromUriOrPath, initWasm };
