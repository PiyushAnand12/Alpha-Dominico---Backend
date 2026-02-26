const express = require('express');
const router  = express.Router();
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// This runs when someone clicks the unsubscribe link: /unsubscribe?token=abc123
router.get('/', async (req, res) => {
  const { token } = req.query;

  if (!token) {
    return res.send(`
      <h3>Invalid unsubscribe link.</h3>
    `);
  }

  // Find the subscriber by their unique token
  const { data: sub } = await supabase
    .from('subscribers')
    .select('id, is_active')
    .eq('unsubscribe_token', token)
    .single();

  if (!sub) {
    return res.send(`
      <h3>Link not found or already used.</h3>
    `);
  }

  if (!sub.is_active) {
    return res.send(`
      <h3>You are already unsubscribed.</h3>
    `);
  }

  // Mark them as inactive
  await supabase
    .from('subscribers')
    .update({
      is_active: false,
      unsubscribed_at: new Date().toISOString()
    })
    .eq('id', sub.id);

  return res.send(`
    <h2>✅ You have been unsubscribed.</h2>
    <p>You will no longer receive daily stock insights.</p>
    <p>If this was a mistake, you can subscribe again from the website.</p>
  `);
});

module.exports = router;