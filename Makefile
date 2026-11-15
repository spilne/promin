.PHONY: release release-dry release-check docs docs-check docs-build docs-dev
export VERSION

release:
	bun run release

release-dry:
	bun run release:dry

release-check:
	bun run release:check

docs:
	bun run documentation:build

docs-check:
	bun run documentation:check

docs-build:
	bun run documentation:build:html

docs-dev:
	bun run docs:dev
