import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fileSystemFromUriOrPath } from '../src/core/filesystem.js';
import { FileType } from '../src/core/types.js';

describe('filesystem readFileAsTable', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each([
    [FileType.CSV, 'csv'],
    [FileType.ORC, 'orc'],
    [FileType.JSON, 'json'],
  ])(
    'rejects unsupported %s payloads on the HTTP reader path before fetch',
    async (fileType, fileTypeName) => {
      const [fs] = fileSystemFromUriOrPath('http://example.test/graphs/');

      await expect(
        fs.readFileAsTable(
          'http://example.test/graphs/chunk0',
          fileType,
          ['id'],
        ),
      ).rejects.toThrow(
        new RegExp(
          `Unsupported payload file type for readFileAsTable: ${fileTypeName}`,
        ),
      );

      expect(fetch).not.toHaveBeenCalled();
    },
  );
});
