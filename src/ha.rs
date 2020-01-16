/*
@author: xiao cai niao
@datetime: 2019/11/15
*/

use crate::storage::rocks::{DbInfo, KeyValue, CfNameTypeCode};
use std::{thread, time};
use crate::ha::procotol::{MyProtocol, MysqlState, ReponseErr};
use crate::storage::opdb::HostInfoValue;
use std::error::Error;
use std::net::{TcpStream, SocketAddr, IpAddr, Ipv4Addr};
use std::time::Duration;

pub mod procotol;
pub mod nodes_manager;
pub mod route_manager;
pub mod sys_manager;
use actix_web::web;
use std::sync::{mpsc};


pub struct DownNodeInfo {
    cluster_name: String,
    host: String,
    dbport: usize,
    online: bool
}

///
/// 缓存各节点信息及当前状态
///
#[derive(Clone, Debug)]
struct NodesInfo {
    key: String,
    value: HostInfoValue,
    cf_name: String,
}

impl NodesInfo {
    fn set_offline(&mut self, db: &web::Data<DbInfo>, sender: &mpsc::Sender<DownNodeInfo>) -> Result<(), Box<dyn Error>> {
        if self.value.online{
            self.value.online = false;
            self.update_value(db)?;
            self.send_down_info(sender, false);
        }
        Ok(())
    }

    fn set_online(&mut self, db: &web::Data<DbInfo>, sender: &mpsc::Sender<DownNodeInfo>) -> Result<(), Box<dyn Error>> {
        if !self.value.online {
            self.value.online = true;
            self.update_value(db)?;
            self.send_down_info(sender, true);
        }
        Ok(())
    }

    fn send_down_info(&self, sender: &mpsc::Sender<DownNodeInfo> , state: bool) {
        if !self.value.maintain {
            let down = DownNodeInfo{host: self.key.clone(), dbport:self.value.dbport.clone(), online: state, cluster_name: self.value.cluster_name.clone()};
            if let Err(e) = sender.send(down){
                info!("{} state send faild: {:?}",self.key,e.to_string());
            }
        }
    }

    fn update_value(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(&self.value)?;
        let a = KeyValue{key: (&self.key).parse()?, value: (&value).parse()? };
        db.put(&a, &self.cf_name)?;
        Ok(())
    }

    ///
    /// 修改当前节点状态数据
    ///
    fn update_nodes_state(&mut self, db: &web::Data<DbInfo>, nodes_state: &MysqlState) -> Result<(), Box<dyn Error>> {
        if self.value.online {
//            let value = serde_json::to_string(nodes_state)?;
//            let a = KeyValue{key: (&self.key).parse()?, value };
//            db.put(&a, &CfNameTypeCode::NodesState.get())?;
            nodes_state.save(db, &self.key)?;
        }
        Ok(())
    }
}

///
/// 所有节点信息
///
#[derive(Debug)]
struct AllNodes {
    info: Vec<NodesInfo>
}

impl AllNodes {
    fn new(db: &web::Data<DbInfo>) -> AllNodes {
        let nodes_info = get_nodes_info(db).unwrap();
        AllNodes{
            info: nodes_info
        }
    }

    fn check_node_state(&mut self, db: &web::Data<DbInfo>, sender: &mpsc::Sender<DownNodeInfo>) {
        for nodes in &mut self.info {
            //if !nodes.value.maintain {
            let state = get_node_state_from_host(&nodes.key);
            match state {
                Ok(v) => {
                    //info!("{:?}", &v);
                    let state;
                    if !v.online {
                        state = nodes.set_offline(&db, sender);
                    }else {
                        state = nodes.set_online(&db, sender);
                    }
                    if let Err(e) = state {
                        info!("update host info failed!!!!");
                        info!("{:?}",e.to_string()) ;
                        continue;
                    }
                    if let Err(e) = nodes.update_nodes_state(&db, &v){
                        info!("{:?}",e.to_string());
                    };
                },
                Err(_e) => {
                    //info!("{} state check failed ({})....",&nodes.key, e.to_string());
                    if let Err(e) = nodes.set_offline(&db, sender){
                        info!("{:?}",e.to_string());
                    };
                }
            }
            //}
        }
    }
}

///
/// 负责所有节点状态检查及高可用管理操作
///
/// 单循环获取每个节点状态信息，每次循环之间sleep 1秒
///
/// 没60次循环之后重新从db中获取所有节点的host信息
///
///
pub fn ha_manager(db: web::Data<DbInfo>,  sender: mpsc::Sender<DownNodeInfo>) {
    let mut start_time = crate::timestamp();
    let mut nodes_info = AllNodes::new(&db);
    //info!("node list: {:?}",nodes_info);
    'all: loop {
        nodes_info.check_node_state(&db, &sender);

        if crate::timestamp() - start_time >= 10000 {
            //每10秒获取一次rocksdb中节点信息
            nodes_info = AllNodes::new(&db);
            //info!("node list: {:?}",nodes_info);
            start_time = crate::timestamp();
        }

        let ten_secs = time::Duration::from_secs(1);
        thread::sleep(ten_secs);
    }
}


///
/// 从rocksdb中获取最新的节点信息
///
fn get_nodes_info(db: &web::Data<DbInfo>) -> Result<Vec<NodesInfo>, Box<dyn Error>> {
    let mut nodes_info: Vec<NodesInfo> = vec![];
    let cf_name = CfNameTypeCode::HaNodesInfo.get();
    let all_nodes_info = db.iterator(&cf_name, &String::from(""))?;
    for row in all_nodes_info{
        let value = serde_json::from_str(&row.value)?;
        let b = NodesInfo{key: row.key, value, cf_name: cf_name.clone()};
        nodes_info.push(b);
    }
    Ok(nodes_info)
}

///
/// 连接节点并接收返回数据，并序列化对应的结构
/// 返回正确的数据只有两种类型
///     procotol::MysqlState
/// 接收到其余类型都直接返回错误
///
fn get_node_state_from_host(host_info: &str) -> Result<MysqlState, Box<dyn Error>> {
    let response_packet = MyProtocol::MysqlCheck.get_packet(&host_info.to_string())?;
    //let type_code = MyProtocol::new(&packet[0]);
    match response_packet.type_code {
        MyProtocol::MysqlCheck => {
            let value: MysqlState = serde_json::from_slice(&response_packet.value)?;
            return Ok(value);
        }
        MyProtocol::Error => {
            let err: ReponseErr = serde_json::from_slice(&response_packet.value)?;
            return Err(err.err.into());
        }
        _ => {
            let a = format!("return invalid type code: {:?}",&response_packet.type_code);
            return Err(a.into());
        }
    }
}

///
///
/// 连接节点并设置读写超时时间
///
/// 连接超时时间为2秒
///
/// 读写超时时间均为10秒
///
fn conn(host_info: &str) -> Result<TcpStream, Box<dyn Error>> {
    let host_info = host_info.split(":");
    let host_vec = host_info.collect::<Vec<&str>>();
    let port = host_vec[1].to_string().parse::<u16>()?;
    let ip_vec = host_vec[0].split(".");
    let ip_vec = ip_vec.collect::<Vec<&str>>();
    let mut ip_info = vec![];
    for i in ip_vec{
        ip_info.push(i.to_string().parse::<u8>()?);
    }
    let addrs = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(ip_info[0], ip_info[1], ip_info[2], ip_info[3])), port));
    //let tcp_conn = TcpStream::connect(host_info)?;
    let tcp_conn = TcpStream::connect_timeout(&addrs, Duration::new(2,5))?;
    tcp_conn.set_read_timeout(Some(Duration::new(10,10)))?;
    tcp_conn.set_write_timeout(Some(Duration::new(10,10)))?;
    Ok(tcp_conn)
}