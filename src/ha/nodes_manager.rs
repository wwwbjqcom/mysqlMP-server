/*
@author: xiao cai niao
@datetime: 2019/11/22
*/

use actix_web::web;
use std::sync::{mpsc, Arc, Mutex};
use crate::storage::rocks::{DbInfo, KeyValue};
use crate::ha::{DownNodeInfo, conn};
use crate::ha::procotol;
use std::error::Error;
use crate::ha::procotol::{HostInfoValue, DownNodeCheckStatus, rec_packet, MyProtocol};
use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};

///
///用于检查状态
///
#[derive(Serialize, Deserialize)]
struct CheckState {
    db_offline: usize,
    client_offline: usize,
    all_nodes: usize,
    db_down: bool,
    client_down: bool,
}
impl CheckState {
    fn new(all_nodes: usize) -> CheckState{
        CheckState{ db_offline: 0, client_offline: 0, all_nodes, db_down: false, client_down: false}
    }
    fn check(&mut self, state: &DownNodeCheckStatus) {
        if state.host.len() > 0 {
            if !state.client_status{
                self.client_offline += 1;
            }
            if !state.db_status {
                self.db_offline += 1;
            }

            if self.db_offline >= (self.all_nodes/2+1) {
                self.db_down = true;
            }
            if self.client_offline >= (self.all_nodes/2+1) {
                self.client_down = true;
            }
        }
    }

    fn update_db(&self, db: &web::Data<DbInfo>, key: &String) -> Result<(), Box<dyn Error>> {
        let cf_name = String::from("Check_state");
        let value = serde_json::to_string(&self)?;
        let a = KeyValue{key: key.clone(), value: (&value).parse()? };
        db.put(&a, &cf_name)?;
        Ok(())
    }

    fn delete_from_db(&self, db: &web::Data<DbInfo>, key: &String) -> Result<(), Box<dyn Error>> {
        let cf_name = String::from("Check_state");
        db.delete(key, &cf_name)?;
        Ok(())
    }
}

///
///主要负责master宕机时新节点选举及切换、追加日志操作
///
pub fn manager(db: web::Data<DbInfo>,  rec: mpsc::Receiver<DownNodeInfo>){
    loop {
        let r = rec.recv().unwrap();
        if !r.online {
            info!("master {:?} is down for cluster {:?}....", r.host, r.cluster_name);
            info!("check network......");
            //let nodes = crate::ha::get_nodes_info(&db);
            let down_node = procotol::DownNodeCheck::new(r.host, r.dbport);
            let state = check_network_for_downnode(&db,&down_node).unwrap();
            if let Err(e) = state.update_db(&db, &down_node.host){
                info!("{:?}",e.to_string());
            };
            info!("mysql server is downd:{}, client is downd:{}", state.db_down, state.client_down);
            info!("Ok");
        }else {
            info!("node: {} recovery success, delete status now...", r.host);
            let state = CheckState::new(0);
            if let Err(e) = state.delete_from_db(&db, &r.host){
                info!("{:?}",e.to_string());
            };
            info!("Ok");
        }
    }
}

///
/// 获取所有存活的client节点，分发检查请求
///
fn check_network_for_downnode(db: &web::Data<DbInfo>, down_node: &procotol::DownNodeCheck) -> Result<CheckState, Box<dyn Error>> {
    let cf_name = String::from("Ha_nodes_info");
    let result = db.iterator(&cf_name,&String::from(""))?;
    let (rt, rc) = mpsc::channel();
    let rt= Arc::new(Mutex::new(rt));
    let mut count = 0 as usize;
    for nodes in result {
        if nodes.key != down_node.host {
            count += 1;
            let state: HostInfoValue = serde_json::from_str(&nodes.value)?;
            if !state.online {
                continue;
            }
            if state.rtype == "route"{
                continue;
            }
            let my_rt = Arc::clone(&rt);
            let my_down_node = down_node.clone();
            thread::spawn(move||{
                get_down_state_from_node(&state.host, &my_down_node, my_rt);
            });
        }
    }

    let mut check_state = CheckState::new(count);
    for _i in 0..count {
        let state = rc.recv_timeout(Duration::new(2,5));
        match state {
            Ok(s) => {
                check_state.check(&s);
            }
            Err(e) => {
                info!("host {} check error: {:}", &down_node.host, e.to_string());
                check_state.all_nodes -= 1;
                continue;
            }
        }
    }
    Ok(check_state)
}


///
/// 请求节点检查状态
///
fn get_down_state_from_node(host_info: &String,
                            down_node: &procotol::DownNodeCheck,
                            sender: Arc<Mutex<mpsc::Sender<DownNodeCheckStatus>>>) {
    let mut conn = conn(host_info).unwrap();
    procotol::send_value_packet(&mut conn, down_node, MyProtocol::DownNodeCheck).unwrap();
    let packet = rec_packet(&mut conn).unwrap();
    let type_code = MyProtocol::new(&packet[0]);
    match type_code {
        MyProtocol::DownNodeCheck => {
            let value: DownNodeCheckStatus = serde_json::from_slice(&packet[9..]).unwrap();
            info!("{:?}",value);
            sender.lock().unwrap().send(value).unwrap();
        }
        _ => {
            info!("return invalid type code: {}",&packet[0]);
        }
    }

}