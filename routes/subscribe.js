const express = require('express');
const router  = express.Router();
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend   = new Resend(process.env.RESEND_API_KEY);

router.post('/', async (req, res) => {
  const email = (req.body.email || '').toLowerCase().trim();

  // 1. Is the email valid?
  if (!email || !email.includes('@')) {
    return res.status(400).json({ success: false, message: 'Please enter a valid email.' });
  }

  // 2. Is this email already subscribed?
  const { data: existing } = await supabase
    .from('subscribers')
    .select('id, is_active')
    .eq('email', email)
    .single();

  if (existing?.is_active) {
    return res.json({ success: true, message: 'You are already subscribed!' });
  }

  // 3. Was unsubscribed before? Re-activate them
  if (existing && !existing.is_active) {
    await supabase.from('subscribers')
      .update({ is_active: true, unsubscribed_at: null })
      .eq('email', email);
    return res.json({ success: true, message: 'Welcome back! You have been re-subscribed.' });
  }

  // 4. Brand new subscriber — save to database
  const { data: newSub, error } = await supabase
    .from('subscribers')
    .insert({ email })
    .select()
    .single();

  if (error) {
    console.error(error);
    return res.status(500).json({ success: false, message: 'Something went wrong. Try again.' });
  }

  // 5. Send welcome email (if this fails, signup still succeeds)
  try {
    await resend.emails.send({
      from: `${process.env.FROM_NAME} <${process.env.FROM_EMAIL}>`,
      to: email,
      subject: '✅ Subscribed to Daily Stock Insights',
      html: `
You are subscribed! Your first report arrives tomorrow.


             
Unsubscribe


             
DISCLAIMER: This is not investment advice. For informational purposes only.

`
    });
  } catch (e) {
    console.error('Welcome email failed (non-fatal):', e.message);
  }

  return res.status(201).json({ success: true, message: 'Subscribed! Check your inbox.' });
});

module.exports = router;