# The image is the static binary and nothing else: there is no base image
# to pin, no shell and no package manager, and Ezu makes no network calls
# of its own. Build the binary first with `make static`.
FROM scratch
COPY target/x86_64-unknown-linux-musl/release/ezu /ezu
USER 65534:65534
# Inside a container, listen on every interface so a published port reaches
# it; the host decides what is exposed.
ENV EZU_LISTEN=0.0.0.0:7600
EXPOSE 7600
ENTRYPOINT ["/ezu"]
