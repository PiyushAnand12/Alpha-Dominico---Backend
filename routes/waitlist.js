const express = require('express');
const crypto = require('crypto');
const path = require('path');

const router = express.Router();

/**
 * Storage: SQLite (same project DB file)
 * This avoids needing Supabase keys and matches your existing SQLite setup.
 *
 * Dependency required:
 *   npm i better-sqlite3
 */
let db = null;
function getDb() {
  if (db) return db;
  // lazy require so server can start even before install (route will error clearly)
  let Database;
  try {
    Database = require('better-sqlite3');
  } catch (e) {
    throw new Error("Missing dependency: better-sqlite3. Run: npm i better-sqlite3");
  }

  const dbPath = process.env.APP_DB_PATH || 'sepa_app.db';
  const abs = path.isAbsolute(dbPath) ? dbPath : path.join(process.cwd(), dbPath);
  db = new Database(abs);

  // Ensure tables exist
  db.exec(`
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS waitlist_leads (
      id            TEXT PRIMARY KEY,
      name          TEXT NOT NULL,
      email         TEXT NOT NULL UNIQUE,
      phone         TEXT NOT NULL,
      consent       INTEGER NOT NULL DEFAULT 1,
      created_at    TEXT NOT NULL,
      utm_source    TEXT DEFAULT NULL,
      utm_medium    TEXT DEFAULT NULL,
      utm_campaign  TEXT DEFAULT NULL,
      utm_term      TEXT DEFAULT NULL,
      utm_content   TEXT DEFAULT NULL,
      referrer      TEXT DEFAULT NULL,
      ip_hash       TEXT DEFAULT NULL,
      status        TEXT NOT NULL DEFAULT 'new'
    );

    CREATE INDEX IF NOT EXISTS idx_waitlist_created_at ON waitlist_leads(created_at);
    CREATE INDEX IF NOT EXISTS idx_waitlist_phone      ON waitlist_leads(phone);

    CREATE TABLE IF NOT EXISTS waitlist_feedback (
      id                  TEXT PRIMARY KEY,
      lead_id             TEXT NOT NULL,
      top_needs           TEXT NOT NULL, -- JSON array string
      delivery_preference TEXT NOT NULL,
      price_expectation   TEXT NOT NULL,
      free_text           TEXT DEFAULT NULL,
      created_at          TEXT NOT NULL,
      FOREIGN KEY (lead_id) REFERENCES waitlist_leads(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_wl_feedback_lead ON waitlist_feedback(lead_id);
  `);

  return db;
}

function bad(res, message, code = 400) {
  return res.status(code).json({ success: false, message });
}

function emailOk(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/i.test(email || '');
}

function normalizePhone(raw) {
  const digits = String(raw || '').replace(/[^\d]/g, '');
  let d = digits;
  if (d.length === 12 && d.startsWith('91')) d = d.slice(2);
  if (d.length === 11 && d.startsWith('0')) d = d.slice(1);
  if (d.length !== 10) return null;
  if (!/^[6-9]\d{9}$/.test(d)) return null;
  return '+91' + d;
}

function getIp(req) {
  const xf = req.headers['x-forwarded-for'];
  if (typeof xf === 'string' && xf.length) return xf.split(',')[0].trim();
  return req.socket?.remoteAddress || '';
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

/**
 * POST /waitlist  (also mounted at /api/waitlist)
 */
router.post('/', (req, res) => {
  let db;
  try {
    db = getDb();
  } catch (e) {
    return bad(res, e.message || 'DB init failed', 500);
  }

  const body = req.body || {};
  const name = String(body.name || '').trim();
  const email = String(body.email || '').trim().toLowerCase();
  const phone = normalizePhone(body.phone);
  const consent = body.consent === true;
  const honeypot = String(body.honeypot || '').trim();

  if (honeypot) return bad(res, 'Bot detected.');
  if (name.length < 2) return bad(res, 'Name is required.');
  if (!emailOk(email)) return bad(res, 'Valid email is required.');
  if (!phone) return bad(res, 'Valid Indian phone number is required.');
  if (!consent) return bad(res, 'Consent is required.');

  // Rate limit: 5 signups / 10 minutes by IP hash
  const ip = getIp(req);
  const salt = process.env.IP_HASH_SALT || '';
  const ipHash = ip ? sha256(ip + salt) : null;

  if (ipHash) {
    const since = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const count = db.prepare(
      'SELECT COUNT(1) AS c FROM waitlist_leads WHERE ip_hash = ? AND created_at >= ?'
    ).get(ipHash, since)?.c || 0;
    if (count >= 5) return bad(res, 'Too many attempts. Try again later.', 429);
  }

  // De-dupe by email
  const existing = db.prepare('SELECT id FROM waitlist_leads WHERE email = ?').get(email);
  if (existing?.id) return res.json({ success: true, lead_id: existing.id });

  const leadId = crypto.randomUUID ? crypto.randomUUID() : sha256(email + Date.now());
  const createdAt = new Date().toISOString();

  db.prepare(`
    INSERT INTO waitlist_leads
      (id, name, email, phone, consent, created_at,
       utm_source, utm_medium, utm_campaign, utm_term, utm_content,
       referrer, ip_hash, status)
    VALUES
      (@id, @name, @email, @phone, 1, @created_at,
       @utm_source, @utm_medium, @utm_campaign, @utm_term, @utm_content,
       @referrer, @ip_hash, 'new')
  `).run({
    id: leadId,
    name,
    email,
    phone,
    created_at: createdAt,
    utm_source: body.utm_source || null,
    utm_medium: body.utm_medium || null,
    utm_campaign: body.utm_campaign || null,
    utm_term: body.utm_term || null,
    utm_content: body.utm_content || null,
    referrer: body.referrer || null,
    ip_hash: ipHash
  });

  return res.json({ success: true, lead_id: leadId });
});

/**
 * POST /waitlist/feedback  (also mounted at /api/waitlist/feedback)
 */
router.post('/feedback', (req, res) => {
  let db;
  try {
    db = getDb();
  } catch (e) {
    return bad(res, e.message || 'DB init failed', 500);
  }

  const body = req.body || {};
  const leadId = String(body.lead_id || '').trim();
  if (!leadId) return bad(res, 'lead_id is required.');

  const exists = db.prepare('SELECT id FROM waitlist_leads WHERE id = ?').get(leadId);
  if (!exists) return bad(res, 'Invalid lead_id.');

  const topNeeds = Array.isArray(body.top_needs) ? body.top_needs : [];
  const delivery = String(body.delivery_preference || 'email').trim() || 'email';
  const price = String(body.price_expectation || '0-199').trim() || '0-199';
  const freeText = body.free_text ? String(body.free_text).slice(0, 500) : null;

  const fbId = crypto.randomUUID ? crypto.randomUUID() : sha256(leadId + Date.now());
  const createdAt = new Date().toISOString();

  db.prepare(`
    INSERT INTO waitlist_feedback
      (id, lead_id, top_needs, delivery_preference, price_expectation, free_text, created_at)
    VALUES
      (@id, @lead_id, @top_needs, @delivery_preference, @price_expectation, @free_text, @created_at)
  `).run({
    id: fbId,
    lead_id: leadId,
    top_needs: JSON.stringify(topNeeds),
    delivery_preference: delivery,
    price_expectation: price,
    free_text: freeText,
    created_at: createdAt
  });

  return res.json({ success: true });
});

module.exports = router;
