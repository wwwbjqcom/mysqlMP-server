/*
@author: xiao cai niao
@datetime: 2019/11/5
*/
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use crate::storage;
use crate::storage::rocks::{DbInfo, KeyValue};
use crate::ha::procotol::{MysqlState, HostInfoValue, AllNodeInfo, ReponseErr};
use std::error::Error;
use crate::ha::nodes_manager::SwitchForNodes;


#[derive(Serialize, Deserialize, Debug)]
pub struct HostInfo {
    pub host: String,   //127.0.0.1:3306
    pub rtype: String,  //db、route
    pub dbport: String, //default 3306
    pub cluster_name: String   //集群名称
}

#[derive(Serialize, Deserialize, Debug)]
pub struct State {
    pub state: u8,
    pub value: String
}

impl State {
    pub fn new() -> HttpResponse {
        HttpResponse::Ok()
            .json(State {
                state: 1,
                value: "OK".parse().unwrap()
            })
    }
}


/// extract `import host info` using serde
pub fn import_mysql_info(data: web::Data<DbInfo>, info: web::Form<HostInfo>) -> HttpResponse {
    let state = storage::opdb::insert_mysql_host_info(data, &info);
    HttpResponse::from(match state {
        Ok(()) => {
            HttpResponse::Ok()
                .json(State{
                    state: 1, value: "OK".parse().unwrap()
                })
        }
        Err(e) => {
            HttpResponse::Ok()
                .json(State{
                    state: 0, value: e
                })
        }
    })
}

/// extract `export mysql host info` using serde
pub fn get_all_mysql_info(data: web::Data<DbInfo>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let result = data.iterator(&cf_name,&String::from(""));
    let mut rows = AllNodeInfo::new();
    match result {
        Ok(v) => {
            for row in v {
                let role = get_nodes_role(&data, &row.key);
                let value: HostInfoValue = serde_json::from_str(&row.value).unwrap();
                let v = crate::ha::procotol::HostInfoValueGetAllState::new(&value, role);
                if &value.rtype == &String::from("route") {
                    rows.cluster_op(String::from("route"), v);
                }else {
                    rows.cluster_op(value.cluster_name, v);
                }
                //rows.push(serde_json::json!(v));
            }
            HttpResponse::Ok()
                .json(rows.rows)
        }
        Err(_e) => {
            HttpResponse::Ok()
                .json(vec![AllNodeInfo::new()])
        }
    }
}

pub fn get_nodes_role(data: &web::Data<DbInfo>, key: &String) -> String {
    let cf_name = String::from("Nodes_state");
    let v = data.get(key, &cf_name);
    match v {
        Ok(value) => {
            if value.value.len() > 0 {
                let re: MysqlState = serde_json::from_str(&value.value).unwrap();
                return re.role;
            }
        }
        Err(e) => {
            info!("{}",e.to_string());
        }
    }
    return String::from("");
}

#[derive(Serialize, Deserialize)]
pub struct EditInfo {
    pub cluster_name: String,
    pub host: String,
    pub dbport: usize,
    pub rtype: String,
    pub role: String,
    pub online: bool,
}

pub fn edit_nodes(data: web::Data<DbInfo>, info: web::Form<EditInfo>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;
    let cur_value = data.get(key, &cf_name);
    match cur_value {
        Ok(v) => {
            let mut db_value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
            db_value.edit(&info);
            let value = serde_json::to_string(&db_value).unwrap();
            let row = KeyValue::new(&key, &value);
            let a = data.put(&row, &cf_name);
            return response(a);
        }
        Err(e) => {
            return ReponseErr::new(e.to_string());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct EditMainTain{
    pub host: String,
    pub maintain: bool
}

pub fn edit_maintain(data: web::Data<DbInfo>, info: web::Form<EditMainTain>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;
    let cur_value = data.get(key, &cf_name);
    match cur_value {
        Ok(v) => {
            let mut db_value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
            db_value.maintain(&info);
            let value = serde_json::to_string(&db_value).unwrap();
            let row = KeyValue::new(&key, &value);
            let a = data.put(&row, &cf_name);
            return response(a);
        }
        Err(e) => {
            return ReponseErr::new(e.to_string());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DeleteNode {
    pub host: String,
}

impl DeleteNode {
    pub fn exec(&self, data: &web::Data<DbInfo>) -> HttpResponse {
        let cf_name = String::from("Ha_nodes_info");
        let cur_value = data.get(&self.host, &cf_name);
        match cur_value {
            Ok(v) => {
                let value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
                if value.maintain {
                    return response(data.delete(&self.host, &cf_name));
                }else {
                    let err = String::from("the maintenance mode node is only deleted");
                    return ReponseErr::new(err);
                }
            }
            Err(e) => {
                return ReponseErr::new(e.to_string());
            }
        }
    }
}

pub fn delete_node(data: web::Data<DbInfo>, info: web::Form<DeleteNode>) -> HttpResponse {
    return info.exec(&data);
}

///
/// 主动切换
///
#[derive(Serialize, Deserialize, Debug)]
pub struct SwitchInfo {
    pub host: String,
}
pub fn switch(data: web::Data<DbInfo>, info: web::Form<SwitchInfo>) -> HttpResponse {
    let mut switch_info = SwitchForNodes::new(&info.host);
    return response(switch_info.switch(&data));
}


fn response(a: Result<(), Box<dyn Error>>) -> HttpResponse {
    match a {
        Ok(()) => {
            return State::new();
        },
        Err(e) => {
            return ReponseErr::new(e.to_string());
        }
    }
}



