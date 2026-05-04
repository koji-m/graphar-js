import { describe, expect, it } from 'vitest';
import { fileSystemFromUriOrPath } from '../src/index.js';

describe('fileSystemFromUriOrPath', () => {
  it('accepts absolute local paths', () => {
    const [, outPath] = fileSystemFromUriOrPath('/tmp/graphar/');

    expect(outPath).toBe('/tmp/graphar/');
  });

  it('accepts file URIs and normalizes them to local paths', () => {
    const [, outPath] = fileSystemFromUriOrPath('file:///tmp/graphar/data');

    expect(outPath).toBe('/tmp/graphar/data');
  });

  it('rejects relative local paths', () => {
    expect(() => fileSystemFromUriOrPath('./graphar')).toThrow(
      /relative local path/,
    );
  });
});
