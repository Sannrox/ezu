.PHONY: all static test test-integration update validate
all:
	cargo build --all-targets --locked
test:
	cargo test --locked
test-integration:
	cargo test --locked --tests
update:
	@for s in $$(ls scripts/update-*.sh 2>/dev/null | sort); do bash $$s; done
validate:
	cargo fmt --check
	cargo clippy --all-targets --locked -- -D warnings
# A fully static Linux binary (musl), the only content of the image.
static:
	cargo build --release --locked --target x86_64-unknown-linux-musl
