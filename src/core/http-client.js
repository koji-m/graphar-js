class HttpClient {
  async get(path, range = null) {
    const url = new URL(path).href;
    const headers = {};

    if (range) {
      headers.Range = `bytes=${range.start}-${range.end}`;
    }

    const response = await fetch(url, { headers });

    if (!response.ok) {
      throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
    }

    return response;
  }
}

export { HttpClient };
