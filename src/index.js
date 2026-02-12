const express = require('"'"'express'"'"');

const app = express();
const port = process.env.PORT || 4000;

app.use(express.json());

app.get('"'"'/'"'"', (_req, res) => {
  res.json({ service: '"'"'cfl-back'"'"', status: '"'"'ok'"'"' });
});

app.get('"'"'/health'"'"', (_req, res) => {
  res.status(200).json({ healthy: true });
});

app.listen(port, '"'"'0.0.0.0'"'"', () => {
  console.log(`cfl-back listening on port ${port}`);
});
