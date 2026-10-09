# Thin wrappers; the logic lives in scripts/dev.sh (usable without make).
.PHONY: help up demo down clean logs dev build test test-integration loadtest lint certs proto

help up demo down clean logs dev build test test-integration loadtest lint certs proto:
	@bash scripts/dev.sh $@ $(ARGS)
