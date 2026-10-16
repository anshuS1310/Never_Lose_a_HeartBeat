CREATE TABLE IF NOT EXISTS frames (
  session TEXT NOT NULL,
  window INTEGER NOT NULL,
  mode TEXT NOT NULL,
  sensor1 TEXT NOT NULL,
  sensor2 TEXT NOT NULL,
  losses TEXT NOT NULL,
  count1 INTEGER NOT NULL,
  count2 INTEGER NOT NULL,
  received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (session,window)
);
CREATE TABLE IF NOT EXISTS results (
  session TEXT NOT NULL,
  window INTEGER NOT NULL,
  mode TEXT NOT NULL,
  body TEXT NOT NULL,
  processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (session,window,mode)
);
CREATE TABLE IF NOT EXISTS terminal_losses (
  session TEXT NOT NULL,
  sensor INTEGER NOT NULL,
  first_idx INTEGER NOT NULL,
  reason TEXT NOT NULL,
  PRIMARY KEY (session,sensor,first_idx,reason)
);
