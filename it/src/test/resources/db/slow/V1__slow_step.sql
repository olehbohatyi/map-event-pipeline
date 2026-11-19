-- Longer than MigrationsSpec's socket timeout: migrations must not be bound by it
select pg_sleep(3);
