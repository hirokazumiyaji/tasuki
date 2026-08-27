-- 000002_vacuum_tuning: per-table autovacuum reloptions and HOT-friendly fillfactor
-- (docs/05-observability.md "PostgreSQL のデッドタプルと Bloat 監視")

-- Autovacuum tuning for high-churn tables (details: docs/05-observability.md).
-- wf_tasks / wf_inbox are rewritten on every claim/complete; wf_instances gets
-- a row update on every advancement. Lower scale factors make autovacuum run
-- before dead tuples pile up; a smaller fillfactor leaves room in each page so
-- these updates stay HOT (no new index entries).
ALTER TABLE wf_tasks SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000,
    fillfactor = 80
);
ALTER TABLE wf_inbox SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000
);
ALTER TABLE wf_instances SET (
    autovacuum_vacuum_scale_factor = 0.05,
    fillfactor = 80
);
ALTER TABLE wf_signal_dedupe SET (
    autovacuum_vacuum_scale_factor = 0.05
);
