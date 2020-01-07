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
use structopt::StructOpt;
use crate::storage::rocks::DbInfo;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;


#[macro_use]
extern crate log;
extern crate log4rs;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Root};
use actix_session::{CookieSession};
//use actix_web::middleware::Logger;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
pub struct Opt {
    #[structopt(long = "port", help="监听端口")]
    pub port: Option<String>,

    #[structopt(long = "listen", help="监听地址，如:127.0.0.1")]
    pub listen: Option<String>,


}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: usize,
    pub listen: String,
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut port = 8099;
        let mut listen = String::from("127.0.0.1");


        match args.port {
            None => {
            },
            Some(t) => port = t.parse().unwrap(),
        }

        match args.listen {
            None => {
            },
            Some(t) => listen = t,
        }

        Ok(Config{
            port,
            listen
        })
    }
}



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

pub fn rand_string() -> String {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .collect();
    return rand_string;
}

pub fn start_web(db: DbInfo) {
    let args = crate::Opt::from_args();
    let conf = crate::Config::new(args).unwrap_or_else(|err|{
        println!("Problem parsing arguments: {}", err);
        std::process::exit(1);
    });
    let listen_info = format!("{}:{}", conf.listen, conf.port);

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

    //route信息管理线程
    let c = rcdb.clone();
    thread::spawn(move||{
        ha::route_manager::manager(c);
    });

    //管理线程
    let c = rcdb.clone();
    thread::spawn(move||{
        ha::sys_manager::manager(c);
    });

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
            .wrap(
                CookieSession::signed(&[0; 32]) // <- create cookie based session middleware
                    .max_age(3600)
                    .secure(false),
            )
            .register_data(rcdb.clone())
            .service(
                web::resource("/index.html")
                    .name("foo") // <- set resource name, then it could be used in `url_for`
                    .guard(guard::Get())
                    .to(webroute::index_file),
            )
            .service(
                web::resource("/login.html")
                    .name("login") // <- set resource name, then it could be used in `url_for`
                    .guard(guard::Get())
                    .to(webroute::login),
            )
            .service(
                web::resource("/routeinfo")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::web_get_route_info)
                    )
            )
            .service(
                web::resource("/getrouteinfo")  // 用于脚本post获取相关集群路由信息
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::get_route_info)
                    )
            )
            .service(
                web::resource("/marksqlinfo")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::mark_sql)
                    )
            )
            .service(
                web::resource("/pushsqlinfo")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::push_sql)
                    )
            )
            .service(
                web::resource("/getmonitorinfo")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_cluster_monitor_status)
                    )
            )
            .service(
                web::resource("/getclusternamelist")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_cluster_list)
                    )
            )
            .service(
                web::resource("/getrouteclusternamelist")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_route_cluster_list)
                    )
            )
            .service(
                web::resource("/getdbhadata")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_cluster_node_info)
                    )
            )
            .service(
                web::resource("/dbhadetail")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_cluster_node_info)
                    )
            )
            .service(
                web::resource("/getsqls")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::get_rollback_sql)
                    )
            )
            .service(
                web::resource("/import")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::import_mysql_info)
                    )
            )
            .service(
                web::resource("/editnode")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::edit_nodes)
                    )
            )
            .service(
                web::resource("/deletenode")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::delete_node)
                    )
            )
            .service(
                web::resource("/setmaintain")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::edit_maintain)
                    )
            )
            .service(
                web::resource("/switch")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::switch)
                    )
            )
            .service(
                web::resource("/edituser")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::edit_user)
                    )
            )
            .service(
                web::resource("/login")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::login)
                    )
            )
            .service(
                web::resource("/loginout")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::route::loginout)
                    )
            )
            .service(
                web::resource("/slavedelaysetting")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::slave_delay_setting)
                    )
            )
            .service(
                web::resource("/getslavedealysetting")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_slave_delay_setting)
                    )
            )
            .service(
                web::resource("/monitorsetting")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::set_monitor_setting)
                    )
            )
            .service(
                web::resource("/getmonitorsetting")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_monitor_setting)
                    )
            )
            .service(
                web::resource("/monitorstatistics")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::new_route::get_cluster_total_monitor)
                    )
            )
            .service(
                web::resource("/getmetricvalue")
                    .route(
                        web::route()
                            .guard(guard::Post())
                            .guard(guard::Header("content-type", "application/json"))
                            .to(webroute::monitor_route::get_metric_value)
                    )
            )
            .route("/", web::get().to(webroute::index))
            .route("/getuserinfo", web::post().to(webroute::route::get_user_info))
            .route("/getmonitormetric", web::post().to(webroute::monitor_route::get_cluster_metric))
            .route("/createuser", web::post().to(webroute::route::create_user))
            .route("/getlogdata", web::post().to(webroute::route::switchlog))
            .route("/{filename:.*}", web::get().to(webroute::index_static))
            .default_service(
                web::route()
                    .guard(guard::Not(guard::Get()))
                    .to(|| HttpResponse::MethodNotAllowed()),
            )
    })
        .bind_ssl(listen_info, builder)
        .unwrap()
        .run()
        .unwrap();
}

