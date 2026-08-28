-- Revert 000002_vacuum_tuning: -1 means "use the global default".
ALTER TABLE wf_tasks RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_cost_limit, fillfactor);
ALTER TABLE wf_inbox RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_cost_limit);
ALTER TABLE wf_instances RESET (autovacuum_vacuum_scale_factor, fillfactor);
ALTER TABLE wf_signal_dedupe RESET (autovacuum_vacuum_scale_factor);
