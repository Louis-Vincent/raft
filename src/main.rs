
pub mod database {
    pub mod commands {
        include!(concat!(env!("OUT_DIR"), "/database.commands.rs"));
    }
}

use std::{os::unix::net::SocketAddr, pin::Pin, time::Duration};

use bytes::BytesMut;
use database::commands::{
    database_client::DatabaseClient, 
    database_server::{Database, DatabaseServer}, 
    CommandReply, 
    GetCommand, 
    KvCommand, 
    PutCommand, 
    Data,
    GetStreamRequest
};
use database::commands::kv_command::Command;
use futures::{Future, FutureExt};
use prost::Message;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::{server, transport::Server, Response, Status};
use tonic::codegen::tokio_stream;
use tonic::transport::Channel;
use rust_rocksdb::{DB, Options};




#[derive(Debug, Default)]
struct MyDatabase {}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<Data, Status>> + Send>>;

#[tonic::async_trait]
impl Database for MyDatabase {

    type ServerStreamingDataStream = ResponseStream;

    async fn send_command(&self, req: tonic::Request<KvCommand>) -> Result<Response<CommandReply>, Status> {
        println!("Got a request: {:?}", req);
        let kvcommand = req.into_inner();
        let command = kvcommand.command.expect("Null command");

        let command_name = match command {
            Command::Put(_) => String::from("put"),
            Command::Delete(_) => String::from("delete"),
            Command::Get(_) => String::from("get"),
        };
        
        let reply = CommandReply {
            status: format!("got {:?}", command_name),
        };
        println!("Server will return {:?}", reply);
        Ok(Response::new(reply)) // Send back our formatted greeting
   }

   async fn server_streaming_data(&self, req: tonic::Request<GetStreamRequest>) -> Result<Response<ResponseStream>, Status> {
        println!("EchoServer::server_streaming_echo");
        println!("\tclient connected from: {:?}", req.remote_addr());

        // creating infinite stream with requested message
        let repeat = std::iter::repeat(Data {
            val: String::from("echo!")
        });

        let mut stream = Box::pin(
            tokio_stream::iter(repeat).throttle(Duration::from_millis(200))
        );

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ServerStreamingDataStream
        ))
   }
}


async fn streaming_echo(client: &mut DatabaseClient<Channel>)
{
    let mut stream = client
        .server_streaming_data(GetStreamRequest {})
        .await
        .unwrap()
        .into_inner();

    // stream is infinite - take just 5 elements and then disconnect
    //let mut stream = stream.take(num);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            Some(item) = stream.next() => {
                println!("\treceived: {}", item.unwrap().val);
            }
        }
    }
    // stream is droped here and the disconnect info is send to server
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 
    // let key = String::from("Hello");
    // let gc = GetCommand { key };
    let addr = "0.0.0.0:5432".parse().unwrap();
    let mydb = MyDatabase::default();

    let server_shutdown_signal = tokio::signal::ctrl_c()
        .map(|result| result.expect("ctrl+c failed"));


    let server_fut = Server::builder()
        .add_service(DatabaseServer::new(mydb))
        .serve_with_shutdown(addr, server_shutdown_signal);

    //server_fut.await?;
    //let client_fut = 

    let server_h = tokio::spawn(server_fut);
    
    let mut client = DatabaseClient::connect("http://0.0.0.0:5432").await?;
    let _ = streaming_echo(&mut client).await;
    // let request1 = tonic::Request::new(KvCommand {
    //     command: Some(Command::Put(PutCommand { key: String::from("key1"), value: String::from("key2") }))
    // });
    
    // let request2 = tonic::Request::new(KvCommand {
    //     command: Some(Command::Get(GetCommand{ key: String::from("key1") }))
    // });

    // let _reply1 = client.send_command(request1).await;
    // let _reply2 = client.send_command(request2).await;

    server_h.await.unwrap();

    //h.into()

    Ok(())
    // let mut buf = BytesMut::with_capacity(gc.encoded_len());
    // let _ = gc.encode(&mut buf).expect("FAIL");
    // println!("hello world, {:?}", buf)
}