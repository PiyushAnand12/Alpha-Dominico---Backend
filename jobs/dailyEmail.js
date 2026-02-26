const cron    = require('node-cron');
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend   = new Resend(process.env.RESEND_API_KEY);

function startDailyJob(getStockHTML) {

  // Runs Monday–Friday at 7:30 AM India time
  // '0 2 * * 1-5' = 2:00 AM UTC = 7:30 AM IST
 cron.schedule('0 2 * * 1-5', async () => {
    console.log('Running daily email job...');

    // 1. Get all active subscribers from database
    const { data: subscribers } = await supabase
      .from('subscribers')
      .select('email, unsubscribe_token')
      .eq('is_active', true);

    if (!subscribers?.length) {
      return console.log('No subscribers. Skipping.');
    }

    // 2. Get today's stock report HTML from your screener
    const stockHTML = await getStockHTML();
    const today     = new Date().toLocaleDateString('en-IN');
    let sent = 0, failed = 0;

    // 3. Send to each subscriber
    for (const sub of subscribers) {
      try {
        await resend.emails.send({
          from: `${process.env.FROM_NAME} <${process.env.FROM_EMAIL}>`,
          to: sub.email,
          subject: `📊 Daily Stock Insights — ${today}`,
          html: `
            

              ${stockHTML}
              

              

                DISCLAIMER: This report is for informational purposes only. 
                It does not constitute investment advice or a buy/sell recommendation. 
                Always consult a SEBI-registered advisor before investing.


                
                  Unsubscribe
                
              


            

          `
        });
        sent++;
        // Small pause between emails to avoid rate limits
        await new Promise(r => setTimeout(r, 200));
      } catch (e) {
        failed++;
        console.error(`Failed: ${sub.email}`, e.message);
      }
    }

    console.log(`Done. Sent: ${sent}, Failed: ${failed}`);

    // 4. Log campaign stats
    await supabase.from('email_logs').insert({
      total_sent: sent,
      campaign_id: `daily-${new Date().toISOString().split('T')[0]}`
    });

  }, { timezone: 'Asia/Kolkata' });

  console.log('✅ Daily email job scheduled (Mon–Fri 7:30 AM IST)');
}

module.exports = { startDailyJob };