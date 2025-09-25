FROM almalinux/9-base AS builder

RUN dnf -y install epel-release && \
    dnf -y localinstall https://pkgs.sysadmins.ws/el9/base/x86_64/raven-release.el9.noarch.rpm && \
    crb enable && \
    dnf -y install gcc-c++ binutils geoipupdate cronie cargo protobuf-compiler protobuf-devel openssl-devel sqlite-devel libpq-devel --enablerepo=raven-modular && \
    rm -rf /var/cache/dnf /var/lib/rpm /usr/share/doc

WORKDIR /work
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . .

RUN cargo build --release
RUN strip target/release/anubis-geo || true

FROM almalinux/9-minimal AS runtime
RUN microdnf -y --setopt=tsflags=nodocs install openssl bash zlib libgcc && \
    microdnf clean all && rm -rf /var/cache/dnf /var/lib/rpm /usr/share/doc

COPY --from=builder /work/target/release/anubis-geo /usr/local/bin/anubis-geo

RUN chmod +x /usr/local/bin/anubis-geo