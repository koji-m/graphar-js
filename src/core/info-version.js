//TODO
class InfoVersion {
  constructor(versionStr) {
    this.versionStr = versionStr;
  }

  static parse(versionStr) {
    if (versionStr === undefined || versionStr === null) {
      return null;
    }
    return new InfoVersion(versionStr);
  }
}

export { InfoVersion };
