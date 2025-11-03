mod proxy_handler;
mod backend_pool;
mod upstream;

use pingora_core::server::Server;
use pingora_core::server::configuration::Opt;
use pingora_core::services::listening::Service;
use tracing::info;

use proxy_handler::ForwardProxy;
use backend_pool::SimpleBackendPool;
use upstream::load_backends_from_file;

fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting Pingora forward proxy server");

    // Setup server
    let opt = Opt::parse_args();
    let mut server = Server::new(Some(opt)).unwrap();
    server.bootstrap();

    // Load proxies from config file
    let backends = load_backends_from_file("config/proxies.txt")
        .expect("Failed to load proxy backends from config/proxies.txt");

    info!("Loaded {} proxy backends", backends.len());

    // Create pool and proxy service
    let pool = SimpleBackendPool::new(backends);
    let proxy = ForwardProxy::new(pool);
    let mut proxy_service = Service::new("Forward TCP proxy".to_string(), proxy);
    proxy_service.add_tcp("0.0.0.0:8890");

    info!("Forward proxy listening on 0.0.0.0:8890");

    // Start server
    server.add_service(proxy_service);
    info!("Server configured, starting main loop");
    server.run_forever();
}
