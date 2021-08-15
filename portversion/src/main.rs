use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn subscriber(portversions: Arc<RwLock<HashMap<u32, (u32, u32)>>>) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Subscribing to channel: {}", "portversion");
    let red = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut con = red.get_connection()?;
    let mut pubsub = con.as_pubsub();
    pubsub.subscribe("portversion")?;
    
    loop {
        let msg = pubsub.get_message()?;
        let hex: String = msg.get_payload()?;
        let port = u32::from_str_radix(&hex[..16], 16).expect("Error");
        let version = u32::from_str_radix(&hex[16..], 16).expect("Error");
        let time = SystemTime::now().duration_since(UNIX_EPOCH).expect("Error").as_secs() as u32;
        let mut portversions = portversions.write().unwrap();
        match portversions.insert(port, (version, time)) {
            Some(m) => if m.0 != version {
                log::info!("Setting port: {} version: {}", port, version)
            },
            None => log::info!("Setting port: {} version: {}", port, version)
        }
    }
}

pub async fn reaper(portversions: Arc<RwLock<HashMap<u32, (u32, u32)>>>, portversion: Arc<RwLock<(u32, u32)>>) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let current = {
            let portversion = portversion.read().unwrap();
            (portversion.0, portversion.1)
        };

        let top = {
            let mut top = current;
            let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Error").as_secs() as u32;
            let mut portversions = portversions.write().unwrap();
            portversions.retain(|_, &mut v| (now - v.1) < 30);
            for (&k, &v) in portversions.iter() {
                if v.0 > top.1 {
                    top = (k, v.0);
                }
            }
            top
        };

        if top.0 != current.0 {
            log::info!("Updating port: {}", top.0);
            let mut portversion = portversion.write().unwrap();
            portversion.0 = top.0;
            portversion.1 = top.1;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

pub async fn listener(portversion: Arc<RwLock<(u32, u32)>>) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting server on: {}", "5000");
    let listener = TcpListener::bind("0.0.0.0:5000").await?;

    loop {
        match listener.accept().await {
            Ok((mut input, _)) => {
                let port = {
                    let current = portversion.read().unwrap();
                    current.0
                };
        
                let mut output = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
        
                log::info!("Opened connection to port: {}", port);
        
                tokio::spawn(async move {
                    let mut buf = vec![0; 1024];
        
                    log::info!("Reading Input");
        
                    loop {
                        match input.read(&mut buf).await {
                            Ok(0) => {
                                log::info!("Done");
                                break;
                            },
                            Ok(n) => {
                                match output.write_all(&buf[..n]).await {
                                    Ok(()) => {
                                        if n < 1024 {
                                            break;
                                        }
                                    },
                                    Err(e) => {
                                        log::info!("Error: {}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                log::info!("Error: {}", e);
                                break;
                            }
                        }
                    }
        
                    log::info!("Reading Output");
        
                    loop {
                        match output.read(&mut buf).await {
                            Ok(0) => {
                                log::info!("Done");
                                break;
                            },
                            Ok(n) => {
                                match input.write_all(&buf[..n]).await {
                                    Ok(()) => {
                                        if n < 1024 {
                                            break;
                                        }
                                    },
                                    Err(e) => {
                                        log::info!("Error: {}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                log::info!("Error: {}", e);
                                break;
                            }
                        }
                    }
                });
            },
            Err(_) => {
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let portversions = Arc::new(RwLock::new(HashMap::new()));
    let portversion = Arc::new(RwLock::new((0, 0)));

    let subscriberports = Arc::clone(&portversions);
    let subscriber_task = tokio::task::spawn(async move {
        if subscriber(subscriberports).await.is_err() {
            log::error!("Subscriber Error");
        } else {
            log::info!("Subscriber Closed");
        }
    });

    let reaperports = Arc::clone(&portversions);
    let reaperportversion = Arc::clone(&portversion);
    let reaper_task = tokio::task::spawn(async move {
        if reaper(reaperports, reaperportversion).await.is_err() {
            log::error!("Reaper Error");
        } else {
            log::info!("Reaper Closed");
        }
    });

    let listenerportversion = Arc::clone(&portversion);
    let listener_task = tokio::task::spawn(async move {
        if listener(listenerportversion).await.is_err() {
            log::error!("Listener Error");
        } else {
            log::info!("Listener Closed");
        }
    });

    let (_, _, _) = tokio::join!(
        subscriber_task,
        reaper_task,
        listener_task
    );
}