/*
@author: xiao cai niao
@datetime: 2019/10/30
*/
pub mod webroute;
pub mod storage;
pub mod ha;
pub mod readvalue;
use std::time::{SystemTime, UNIX_EPOCH};
use actix_web::{web, App, HttpServer};
use actix_web::{guard, HttpResponse};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use std::sync::{mpsc};
use std::thread;
use crate::storage::rocks::DbInfo;


#[macro_use]
extern crate log;
extern crate log4rs;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Root};

fn init_log() {
    let stdout = ConsoleAppender::builder().build();

    let requests = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("log/requests.log")
        .unwrap();

    let config = log4rs::config::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("requests", Box::new(requests)))
        .logger(log4rs::config::Logger::builder().build("app::backend::db", LevelFilter::Info))
        .logger(log4rs::config::Logger::builder()
            .appender("requests")
            .additive(false)
            .build("app::requests", LevelFilter::Info))
        .build(Root::builder().appender("requests").build(LevelFilter::Info))
        .unwrap();
    log4rs::init_config(config).unwrap();
}


pub fn timestamp() -> i64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let ms = since_the_epoch.as_secs() as i64 * 1000i64 + (since_the_epoch.subsec_nanos() as f64 / 1_000_000.0) as i64;
    ms
}

pub fn start_web(db: DbInfo) {
//    std::env::set_var("RUST_LOG", "actix_web=info");
//    env_logger::init();
    init_log();
    info!("Start......");
    //let db = Arc::new(db);
    let rcdb = web::Data::new(db);

    //状态检查线程与宕机切换线程之间同步状态信息的channel
    let (state_tx, state_rx) = mpsc::channel();

    //节点状态检查线程
    let a = rcdb.clone();
    thread::spawn(move ||{
        ha::ha_manager(a, state_tx);
    });

    //宕机切换恢复管理线程
    let b = rcdb.clone();
    thread::spawn(move ||{
        ha::nodes_manager::manager(b, state_rx);
    });

    //slave节点管理线程



    //web服务
    let mut builder =
        SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file("key.pem", SslFiletype::PEM)
        .unwrap();
    builder.set_certificate_chain_file("cert.pem").unwrap();

    HttpServer::new(move|| {
        App::new()
//            .wrap(Logger::default())
//            .wrap(Logger::new("%a %s %{User-Agent}i"))
            .register_data(rcdb.clone())
            .service(
                web::resource("/index.html")
                    .name("foo") // <- set resource name, then it could be used in `url_for`
                    .guard(guard::Get())
                    .to(webroute::index_file),
            )
            .route("/", web::get().to(webroute::index))
            .route("/pages/tables/import", web::post().to(webroute::route::import_mysql_info))
            .route("/pages/tables/getallmysqlinfo", web::post().to(webroute::route::get_all_mysql_info))
            .route("/pages/tables/editnode", web::post().to(webroute::route::get_all_mysql_info))
            .route("/pages/tables/deletenode", web::post().to(webroute::route::get_all_mysql_info))
            .route("/pages/tables/setmaintain", web::post().to(webroute::route::get_all_mysql_info))
            .route("/{filename:.*}", web::get().to(webroute::index_static))
            .default_service(
                web::route()
                    .guard(guard::Not(guard::Get()))
                    .to(|| HttpResponse::MethodNotAllowed()),
            )
    })
        .bind_ssl("127.0.0.1:8080", builder)
        .unwrap()
        .run()
        .unwrap();
}

