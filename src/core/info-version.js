const VERSION_TO_TYPES = new Map([
  [1, ['bool', 'int32', 'int64', 'float', 'double', 'string']],
]);

function trimTypeName(typeName) {
  return typeName.trim();
}

class InfoVersion {
  constructor(version, userDefineTypes = []) {
    if (!VERSION_TO_TYPES.has(version)) {
      throw new Error(`Unsupported version: ${version}`);
    }
    this.version_ = version;
    this.userDefineTypes_ = userDefineTypes;
    this.versionStr = this.toString();
  }

  version() {
    return this.version_;
  }

  userDefineTypes() {
    return this.userDefineTypes_;
  }

  toString() {
    let versionStr = `gar/v${this.version_}`;
    if (this.userDefineTypes_.length > 0) {
      versionStr += ` (${this.userDefineTypes_.join(',')})`;
    }
    return versionStr;
  }

  checkType(typeStr) {
    const types = VERSION_TO_TYPES.get(this.version_);
    return types.includes(typeStr) || this.userDefineTypes_.includes(typeStr);
  }

  static parse(versionStr) {
    if (versionStr === undefined || versionStr === null) {
      return null;
    }

    const versionMatch = versionStr.match(/^gar\/v(\d+).*/);
    if (!versionMatch) {
      throw new Error(`Invalid version string: ${versionStr}`);
    }

    const version = Number.parseInt(versionMatch[1], 10);
    if (!VERSION_TO_TYPES.has(version)) {
      throw new Error(`Invalid version string: ${versionStr}`);
    }

    const userDefineTypes = [];
    const userDefineTypesMatch = versionStr.match(/^gar\/v\d+ *\((.*)\).*/);
    if (userDefineTypesMatch) {
      for (const typeName of userDefineTypesMatch[1].split(',')) {
        const trimmedTypeName = trimTypeName(typeName);
        if (trimmedTypeName.length > 0) {
          userDefineTypes.push(trimmedTypeName);
        }
      }
    }

    return new InfoVersion(version, userDefineTypes);
  }
}

export { InfoVersion };
