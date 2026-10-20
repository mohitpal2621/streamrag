// Read per request on the server, so one build can point at any API. The browser gets the same URL.
export const apiUrl = () => process.env.API_URL ?? 'http://localhost:4000/graphql';
