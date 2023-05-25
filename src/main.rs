use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context as _, Result};
use arc_swap::ArcSwap;
use clap::Parser;
use my_proxy::{
    config::Config,
    context::{Context, SharedContext},
};
use tokio::signal::unix::SignalKind;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct ProxyArgs {
    #[clap(short, long, env = "MY_PROXY_CONFIG_FILE")]
    config: PathBuf,
    #[clap(long, env = "MY_PROXY_THREADS")]
    threads: Option<usize>,
}

async fn reload(context: &SharedContext, path: &Path) -> Result<()> {
    let config = tokio::fs::read_to_string(path)
        .await
        .context("reading config file")?;
    let config = toml::from_str(&config).context("parsing config file")?;
    let new_context = Context::from_config_and_previous(config, &context.load())?;
    context.store(Arc::new(new_context));
    Ok(())
}

fn main() -> Result<()> {
    let args = ProxyArgs::parse();
    env_logger::init();

    let config_path = args.config;
    let config = std::fs::read_to_string(&config_path).context("reading config file")?;
    let config: Config = toml::from_str(&config).context("parsing config file")?;
    let context = Arc::new(ArcSwap::from_pointee(Context::from_config(config)?));

    let p: usize = std::thread::available_parallelism()?.into();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.unwrap_or(p))
        .enable_all()
        .build()?;

    let context1 = context.clone();
    rt.spawn(async move {
        let mut hup = tokio::signal::unix::signal(SignalKind::hangup()).unwrap();
        loop {
            hup.recv().await.unwrap();
            match reload(&context1, &config_path).await {
                Ok(_) => log::info!("config reloaded"),
                Err(e) => log::warn!("failed to reload: {:#}", e),
            }
        }
    });

    rt.block_on(my_proxy::server::serve(context))?;

    Ok(())
}
