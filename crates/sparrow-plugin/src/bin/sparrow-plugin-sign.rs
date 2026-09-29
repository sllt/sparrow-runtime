//! Offline signing utility. Private keys are never sent to Sparrow Server.
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::signature::{Ed25519KeyPair, KeyPair};
use sparrow_plugin::{
    trust::{signing_message, Signature, TrustPolicy},
    Manifest,
};
use std::{
    io::{Read, Write},
    path::Path,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn read(path: &Path, private: bool) -> Result<Vec<u8>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > 64 * 1024 {
        return Err("input file rejected".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if private && (meta.mode() & 0o077 != 0 || meta.uid() != unsafe { libc::geteuid() }) {
            return Err("private key must be user-owned and mode 0600".into());
        }
    }
    let mut bytes = vec![];
    file.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err("input too large".into());
    }
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["keygen",private,public]=>{
            if Path::new(private).exists()||Path::new(public).exists()||private==public{return Err("key outputs must be distinct new files".into());}
            let key=Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).map_err(|_|"key generation failed")?;
            let pair=Ed25519KeyPair::from_pkcs8(key.as_ref()).map_err(|_|"key decoding failed")?;
            write(Path::new(private),key.as_ref())?;
            let value=serde_json::json!({"algorithm":"ed25519","public_key_base64":STANDARD.encode(pair.public_key().as_ref())});
            if let Err(error)=write(Path::new(public),&serde_json::to_vec_pretty(&value)?) {
                // We created this private file in this command; don't leave an
                // incomplete keypair after a failed public-file publication.
                let _=std::fs::remove_file(private);return Err(error);
            }
        },
        ["sign",manifest,key_id,private,output]=>{
            let manifest:Manifest=serde_json::from_slice(&read(Path::new(manifest),false)?)?;
            let bytes=read(Path::new(private),true)?;
            let pair=Ed25519KeyPair::from_pkcs8(&bytes).map_err(|_|"invalid private Ed25519 PKCS8")?;
            let signature=Signature{algorithm:"ed25519".into(),key_id:(*key_id).into(),
                signature_base64:STANDARD.encode(pair.sign(&signing_message(&manifest,key_id)?).as_ref())};
            write(Path::new(output),&serde_json::to_vec_pretty(&signature)?)?;
        },
        ["verify",manifest,policy,signature]=>{
            let manifest:Manifest=serde_json::from_slice(&read(Path::new(manifest),false)?)?;
            let signature:Signature=serde_json::from_slice(&read(Path::new(signature),false)?)?;
            TrustPolicy::from_file(Path::new(policy))?.verify(&manifest,Some(&signature))?;
        },
        _=>return Err("usage: sparrow-plugin-sign keygen NEW_PRIVATE NEW_PUBLIC | sign MANIFEST KEY_ID PRIVATE NEW_SIGNATURE | verify MANIFEST TRUST_POLICY SIGNATURE".into()),
    }
    println!("{{\"ok\":true}}");
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
