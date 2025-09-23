fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/iptoasn/v1/iptoasn.proto",
                "proto/reputation/v1/reputation.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
