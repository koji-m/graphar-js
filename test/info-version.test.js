import { describe, expect, it } from 'vitest';
import { InfoVersion } from '../src/core/info-version.js';

describe('InfoVersion', () => {
  it('parses the default GraphAr v1 version string', () => {
    const infoVersion = InfoVersion.parse('gar/v1');

    expect(infoVersion.version()).toBe(1);
    expect(infoVersion.userDefineTypes()).toEqual([]);
    expect(infoVersion.toString()).toBe('gar/v1');
    expect(infoVersion.checkType('int32')).toBe(true);
    expect(infoVersion.checkType('date32')).toBe(false);
  });

  it('parses user-defined types from the version string', () => {
    const infoVersion = InfoVersion.parse('gar/v1 (t1,t2)');

    expect(infoVersion.version()).toBe(1);
    expect(infoVersion.userDefineTypes()).toEqual(['t1', 't2']);
    expect(infoVersion.toString()).toBe('gar/v1 (t1,t2)');
    expect(infoVersion.checkType('t1')).toBe(true);
  });

  it('trims whitespace around user-defined type names', () => {
    const infoVersion = InfoVersion.parse('gar/v1 ( t1, t2 )');

    expect(infoVersion.version()).toBe(1);
    expect(infoVersion.userDefineTypes()).toEqual(['t1', 't2']);
    expect(infoVersion.toString()).toBe('gar/v1 (t1,t2)');
  });

  it('rejects unsupported GraphAr versions', () => {
    expect(() => InfoVersion.parse('gar/v2')).toThrow(
      /Invalid version string|Unsupported GraphAr version/,
    );
  });

  it('rejects malformed version strings', () => {
    expect(() => InfoVersion.parse('v1')).toThrow(/Invalid version string/);
  });

  it('returns null for an omitted version to match optional YAML fields', () => {
    expect(InfoVersion.parse(undefined)).toBeNull();
    expect(InfoVersion.parse(null)).toBeNull();
  });
});
