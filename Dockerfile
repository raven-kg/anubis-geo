FROM almalinux/9-base

RUN dnf -y install epel-release && \
    dnf -y localinstall https://pkgs.sysadmins.ws/el9/base/x86_64/raven-release.el9.noarch.rpm && \
    crb enable && \
    dnf -y install gcc-c++ binutils geoipupdate cronie protobuf-compiler protobuf-devel openssl-devel sqlite-devel libpq-devel && \
    rm -rf /var/lib/{dnf,rpm} && rm -rf /usr/share/doc && \
    curl https://sh.rustup.rs -sSf | bash -s -- -y

WORKDIR /work
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . .

RUN cargo build --release && \
    cp target/release/anubis-geo /usr/local/bin/anubis-geo && \
    cargo clean && \
    rm -fr /work/*

