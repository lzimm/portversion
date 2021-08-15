use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};

pub async fn subscriber(portversions: Arc<RwLock<HashMap<u32, (u32, u32)>>>) -> Result<(), Box<dyn std::error::Error>> {
    let red = redis::Client::open("redis://127.0.0.1:5432")?;
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
        portversions.insert(port, (version, time));
    }
}

pub async fn reaper(portversions: Arc<RwLock<HashMap<u32, (u32, u32)>>>, portversion: Arc<RwLock<(u32, u32)>>) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Error").as_secs() as u32;
        let mut portversions = portversions.write().unwrap();
        portversions.retain(|_, &mut v| (now - v.1) < 30);

        let current = portversion.read().unwrap();
        let mut top = (current.0, current.1);
        for (&k, &v) in portversions.iter() {
            if v.0 > current.1 {
                top = (k, v.0);
            }
        }

        let mut portversion = portversion.write().unwrap();
        portversion.0 = top.0;
        portversion.1 = top.1;

        sleep(Duration::from_millis(100)).await;
    }
}

pub async fn listener(portversion: Arc<RwLock<(u32, u32)>>) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    loop {
        let (mut input, _) = listener.accept().await?;

        let current = portversion.read().unwrap();
        let mut output = TcpStream::connect(format!("127.0.0.1:{}", current.0)).await?;

        tokio::spawn(async move {
            let mut buf = vec![0; 1024];

            loop {
                match input.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if output.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        return;
                    }
                }
            }

            loop {
                match output.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if input.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        return;
                    }
                }
            }
        });
    }
}

#[tokio::main]
async fn main() {
    let portversions = Arc::new(RwLock::new(HashMap::new()));
    let portversion = Arc::new(RwLock::new((0, 0)));

    let subscriberports = Arc::clone(&portversions);
    let subscribertask = tokio::spawn(async move {
        subscriber(subscriberports);
    });

    let reaperports = Arc::clone(&portversions);
    let reaperportversion = Arc::clone(&portversion);
    let repaertask = tokio::spawn(async move {
        reaper(reaperports, reaperportversion);
    });
    

    let listenerportversion = Arc::clone(&portversion);
    let listenertask = tokio::spawn(async move {
        listener(listenerportversion);
    });
}