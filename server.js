const express   = require('express');
const cors      = require('cors');
require('dotenv').config();

const subscribeRoute   = require('./routes/subscribe');
const unsubscribeRoute = require('./routes/unsubscribe');
const { startDailyJob } = require('./jobs/dailyEmail');

const app = express();

// Allow your frontend to talk to this backend
app.use(cors({ origin: process.env.FRONTEND_URL || '*' }));
app.use(express.json());

// Routes
app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.use('/subscribe',   subscribeRoute);
app.use('/unsubscribe', unsubscribeRoute);

// Start server
const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);

  // TODO: Replace this with your actual SEPA screener output
  const getStockHTML = async () => {
    return `
      <h1>Your stock insights for today</h1>
      <p>Content goes here.</p>
    `;
  };

  startDailyJob(getStockHTML);
});