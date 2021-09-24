use structopt::StructOpt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, StructOpt)]
#[structopt(name="portversion")]
struct Opt {
    #[structopt(short, long, default_value="5000")]
    port: u32,

    #[structopt(short, long, default_value="portversion")]
    channel: String
}

pub async fn subscriber(channel: String, portversions: Arc<RwLock<HashMap<u32, (u32, u32)>>>) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Subscribing to channel: {}", channel);
    let red = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut con = red.get_connection()?;
    let mut pubsub = con.as_pubsub();
    pubsub.subscribe(channel)?;
    
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
            portversions.retain(|_, &mut (_, ts)| (now - ts) < 30);
            for (&k, &(v, _)) in portversions.iter() {
                if v > top.1 {
                    top = (k, v);
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

pub async fn listener(port: u32, portversion: Arc<RwLock<(u32, u32)>>) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting server on: {}", port);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;

    async fn pipe(src: &mut tokio::io::ReadHalf<tokio::net::TcpStream>, dest: &mut tokio::io::WriteHalf<tokio::net::TcpStream>) {
        let mut buf = vec![0; 1024];
        loop {
            match src.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    match dest.write_all(&buf[..n]).await {
                        Ok(()) => continue,
                        Err(e) => {
                            log::error!("Error: {}", e);
                            break;
                        }
                    }
                },
                Err(e) => {
                    log::error!("Error: {}", e);
                    break;
                }
            }
        }
    }

    loop {
        match listener.accept().await {
            Ok((input, _)) => {
                let (mut inread, mut inwrite) = tokio::io::split(input);

                let port = {
                    let current = portversion.read().unwrap();
                    current.0
                };
        
                match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
                    Ok(output) => {
                        log::info!("Opened connection to port: {}", port);
        
                        tokio::spawn(async move {
                            let (mut outread, mut outwrite) = tokio::io::split(output);

                            let request = pipe(&mut inread, &mut outwrite);
                            let response = pipe(&mut outread, &mut inwrite);

                            tokio::join!(request, response);

                            if outwrite.shutdown().await.is_err() {
                                log::error!("Error closing output socket");
                            }
                            if inwrite.shutdown().await.is_err() {
                                log::error!("Error closing input socket");
                            }
                        });
                    },
                    Err(e) => {
                        log::error!("Error: {}", e);
                        if inwrite.shutdown().await.is_err() {
                            log::error!("Error closing socket");
                        }
                        continue;
                    }
                }
            },
            Err(_) => {
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();

    let opt = Opt::from_args();

    let portversions = Arc::new(RwLock::new(HashMap::new()));
    let portversion = Arc::new(RwLock::new((0, 0)));

    let subscriberchannel = opt.channel;
    let subscriberports = Arc::clone(&portversions);
    let subscriber_task = tokio::task::spawn(async move {
        if subscriber(subscriberchannel, subscriberports).await.is_err() {
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

    let listenerport = opt.port;
    let listenerportversion = Arc::clone(&portversion);
    let listener_task = tokio::task::spawn(async move {
        if listener(listenerport, listenerportversion).await.is_err() {
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