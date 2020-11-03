#[macro_use]
extern crate serde_derive;
mod config;

use native_tls::{TlsConnector, TlsStream, Protocol};
use std::io::Write;
use std::net::TcpStream;
use std::fs::File;
use std::time::{Duration};
use mqtt::{Encodable, Decodable};
use mqtt::packet::*;
use mqtt::{TopicName};
use mqtt::control::variable_header::ConnectReturnCode;
use clap::{Arg, App};
use config::read_config;
use anyhow::Result;

use async_std::prelude::*;
use async_std::stream;

use yahoo_finance_api as yahoo;

async fn connect(broker: String,
           username: String,
           password: String,
           client_id: String,
           verify_name: String)
           -> Result<TlsStream<TcpStream>> {
    let connector = TlsConnector::builder().min_protocol_version(Some(Protocol::Tlsv12)).build()?;
    let stream = TcpStream::connect(&broker)?;
    let mut stream = connector.connect(&verify_name, stream)?;
    let mut conn = ConnectPacket::new("MQTT", &client_id);

    conn.set_clean_session(true);
    conn.set_user_name(Some(username).to_owned());
    conn.set_password(Some(password.to_owned()));
    conn.set_client_identifier(client_id);

    let mut buf = vec![];
    conn.encode(&mut buf)?;
    stream.write_all(&buf[..])?;
    let connack = ConnackPacket::decode(&mut stream)?;
    if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
        panic!("Failed to connect to server, return code {:?}",
               connack.connect_return_code());
    }
    Ok(stream)
}

async fn publish(stream: &mut TlsStream<TcpStream>, msg: String, topic: TopicName) {
    let packet = PublishPacket::new(topic, QoSWithPacketIdentifier::Level1(10), msg);
    let mut buf = vec![];
    packet.encode(&mut buf).unwrap();
    stream.write_all(&buf).unwrap();
}

#[async_std::main]
async fn main() {

    let matches = App::new("MQTT publisher")
        .version("0.2.0")
        .author("Claus Matzinger. <claus.matzinger+kb@gmail.com>")
        .about("Sends data to an MQTT broker")
        .arg(Arg::with_name("ticker_symbol")
                 .short("s")
                 .long("symbol")
                 .help("Get symbol data for this symbol")
                 .default_value("BTC-USD")
                 .takes_value(true))
        .arg(Arg::with_name("config")
                 .short("c")
                 .long("config")
                 .help("Sets a custom config file [default: config.toml]")
                 .default_value("config.toml")
                 .takes_value(true))
        .get_matches();

    let cf = matches.value_of("config").unwrap();
    let mut f = File::open(cf)
        .expect(&format!("Can't open configuration file: {}", cf));
    let settings = read_config(&mut f).await.expect("Can't read configuration file.");
    println!("Connecting to mqtts://{}", settings.mqtt.broker_address);

    let topic_name = TopicName::new(settings.mqtt.topic.clone()).unwrap();

    let mut mqtt_stream = connect(settings.mqtt.broker_address,
                             settings.mqtt.username,
                             settings.mqtt.password,
                             settings.mqtt.client_id,
                             settings.mqtt.broker).await.unwrap();


    let ticker_symbol = matches.value_of("ticker_symbol").unwrap().to_owned();

    let mut interval = stream::interval(Duration::from_millis(1));
    // WHO CARES? IT COMPILES!!!!!!!
    while let Some(_) = interval.next().await {
        let provider = yahoo::YahooConnector::new();
        if let Ok(response) = provider.get_latest_quotes(&ticker_symbol, "1m").await {
            let quote = response.last_quote().unwrap();
            let data = (quote.timestamp, quote.open);
            let msg = format!("{{ \"timestamp\": {}, \"open\": {} }}", data.0, data.1);
            //println!("{}", msg);
            publish(&mut mqtt_stream, msg, topic_name.clone() ).await;
        }    
    }
}
