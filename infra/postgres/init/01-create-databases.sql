-- shared postgres instance backs both the nessie version store and dagster
-- run/event storage; separate logical databases keep them from stepping on
-- each other while avoiding a second container on a memory-constrained host.
CREATE DATABASE nessie;
CREATE DATABASE dagster;
